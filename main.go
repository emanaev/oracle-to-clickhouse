package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

var cfg Config

type Config struct {
	oracle struct {
		databaseImport struct {
			dsn      string `yaml:"dsn"`
			database string `yaml:"database"`
		} `yaml:"database_import"`
		fileImport struct {
			fileLocation string `yaml:"file_location"`
		} `yaml:"file_import"`
	} `yaml:"oracle"`
	clickhouse struct {
		tablePrefix  string `yaml:"table_prefix"`
		fileLocation string `yaml:"file_location"`
	} `yaml:"clickhouse"`
	general struct {
		debug bool `yaml:"debug"`
	} `yaml:"general"`
}

var (
	flagDebug  bool
	flagConfig string
)

// TODO: add more data types and find a solution for the precision problem
var ora2ChConversion = map[string]string{
	"ANYDATA":                  "String",
	"BINARY_DOUBLE":            "String",
	"BLOB":                     "String",
	"CHAR":                     "String",
	"CLOB":                     "String",
	"COL_CLS_LIST":             "String",
	"DATE":                     "DateTime",
	"DS_VARRAY_4_CLOB":         "String",
	"FLOAT":                    "Float128",
	"LONG":                     "Int128",
	"LONGRAW":                  "Int128",
	"NUMBER":                   "Decimal256(30)",
	"NVARCHAR2":                "String",
	"RAW":                      "String",
	"ROWID":                    "Int128",
	"SDO_DIM_ARRAY":            "String",
	"SDO_GEOMETRY":             "String",
	"SDO_NUMBER_ARRAY":         "String",
	"SDO_ORGSCL_TYPE":          "String",
	"SDO_STRING_ARRAY":         "String",
	"TIMESTAMP(0)":             "DateTime",
	"TIMESTAMP(3)":             "DateTime",
	"TIMESTAMP(3)WITHTIMEZONE": "DateTime",
	"TIMESTAMP(6)":             "DateTime",
	"TIMESTAMP(6)WITHTIMEZONE": "DateTime",
	"TIMESTAMP(9)":             "DateTime",
	"UNDEFINED":                "String",
	"VARCHAR2":                 "String",
	"XMLTYPE":                  "String",
}

type oracleDbSchema struct {
	owner         string
	tableName     string
	columnName    string
	dataType      string
	dataLength    int
	dataPrecision int
	nullable      bool
}

func init() {
	flag.BoolVar(&flagDebug, "debug", false, "debug mode")
	flag.StringVar(&flagConfig, "config", "config.yaml", "config file")

	flag.Parse()

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	f, err := os.Open(flagConfig)
	if err != nil {
		log.Panic().Err(err).Msg("Couldn't open config file")
	}

	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&cfg)
	if err != nil {
		log.Panic().Err(err).Msg("Error while parsing config file")
	}
	err = f.Close()
	if err != nil {
		log.Panic().Err(err).Msg("Error while closing file handler")
	}
}

func main() {

	var data []oracleDbSchema

	flagDebug = true

	if flagDebug {
		cfg.oracle.fileImport.fileLocation = "output_example"
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	if cfg.oracle.fileImport.fileLocation != "" && cfg.oracle.databaseImport.dsn != "" {
		log.Panic().Msg("Please provide at least 'database_import' and optionally 'file_import' in your config")
	} else if cfg.oracle.fileImport.fileLocation != "" {
		data = connectByFile(cfg.oracle.fileImport.fileLocation)
	} else if cfg.oracle.fileImport.fileLocation == "" {
		data = connectByODBC(cfg.oracle.databaseImport.dsn)
	} else {
		log.Panic().Msg("Please either provide at least 'database_import' in your config")
	}

	clickhouseQuery := generateClickhouseQuery(data)
	saveClickhouseQuery(clickhouseQuery)
}

func connectByFile(filename string) []oracleDbSchema {
	var fileContent string = openFile(filename)
	var data []oracleDbSchema
	log.Debug().Str("file content", fileContent)

	if fileContent == "" {
		log.Error().Msg("No file found...")
		os.Exit(2)
	}
	data = parseFile(fileContent)
	return data
}

func connectByODBC(dsn string) []oracleDbSchema {
	// TODO: test if odbc connection works
	var queryResult oracleDbSchema
	var allQueryResult []oracleDbSchema
	var err error

	// Open Connection. Provide DSN, Username and Password
	db, err := sql.Open("odbc", fmt.Sprintf("DSN=%v", dsn))
	if err != nil {
		log.Panic().Err(err).Msg("Couldn't connect to database")
	} else {
		log.Debug().Msg("Connection to DB successful")
	}

	// Provide the Query to execute
	rows, err := db.Query("SELECT * from all_tab_columns")
	if err != nil {
		log.Panic().Err(err).Msg("Unable to query")
	}

	// Parse the Result set
	for rows.Next() {
		err = rows.Scan(&queryResult.owner, &queryResult.tableName, &queryResult.columnName, &queryResult.dataType, nil, nil, &queryResult.dataLength, &queryResult.dataPrecision, nil, &queryResult.nullable)
		if err != nil {
			log.Error().Err(err).Msg("Error while parsing result")
		}
		allQueryResult = append(allQueryResult, queryResult)
	}

	// Close the connection
	err = rows.Close()
	if err != nil {
		log.Err(err)
	}
	err = db.Close()
	if err != nil {
		log.Err(err)
	}

	return allQueryResult
}

func saveClickhouseQuery(sqlQuery string) {
	var err error
	var f *os.File
	f, err = os.OpenFile(cfg.clickhouse.fileLocation, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Error().Err(err).Msg("Couldn't create file")
	}
	_, err = f.WriteString(sqlQuery)
	if err != nil {
		log.Error().Err(err).Msg("Couldn't write into file")
	}
	err = f.Sync()
	if err != nil {
		log.Error().Err(err).Msg("Couldn't synchronize with disk")
	}
	err = f.Close()
	if err != nil {
		log.Error().Err(err).Msg("Couldn't close file")
	}
}

func generateClickhouseQuery(oracleDbSchemas []oracleDbSchema) string {
	var sqlTablePrefix string = "ora_"
	var sqlColumns string
	var sqlHead string
	var sqlFoot string
	var sqlDropTable string
	var sqlQuery string

	allTableNames := getAllTableNames(oracleDbSchemas)
	log.Debug().Interface("allTableNames", allTableNames)

	for _, allTableName := range allTableNames {
		log.Debug().Str("allTableName", allTableName.tableName)
		if allTableName.owner != "REPLACE" {
			continue
		}
		for _, item := range oracleDbSchemas {

			log.Debug().Str("oracleDbSchemas", item.tableName)
			sqlDropTable = fmt.Sprintf("DROP TABLE IF EXISTS %v%v;", sqlTablePrefix, allTableName.tableName)
			sqlHead = fmt.Sprintf("CREATE TABLE %v%v (", sqlTablePrefix, allTableName.tableName)
			sqlFoot = fmt.Sprintf(") ENGINE = ODBC('DSN=%v', '%v', '%v');", cfg.oracle.databaseImport.dsn, item.owner, allTableName.tableName) // TODO: Config of REPLACE
			if item.tableName == allTableName.tableName {
				sqlColumns += fmt.Sprintf("`%v` %v,", item.columnName, ora2ChConversion[item.dataType])
				log.Debug().Str("sqlHead", sqlHead).Str("sqlFoot", sqlFoot)
			}

		}
		if sqlColumns != "" {
			// cut off trailing comma in sql query
			sqlColumns = sqlColumns[:len(sqlColumns)-1]
			sqlQuery += sqlDropTable + "\n" + sqlHead + sqlColumns + sqlFoot + "\n\n"
			log.Debug().Str("sqlQuery", sqlQuery)
			sqlHead = ""
			sqlColumns = ""
			sqlFoot = ""
		}

	}
	return sqlQuery
}

func getAllTableNames(oracleDbSchemas []oracleDbSchema) []oracleDbSchema {
	var allTableNames []oracleDbSchema
	m := make(map[string]int)
	for _, item := range oracleDbSchemas {
		if _, ok := m[item.tableName]; ok {
			log.Debug().Msg("duplicate table key found")
		} else {
			log.Debug().Msg("new table key found")
			m[item.tableName] = 1
			allTableNames = append(allTableNames, item)
		}
	}

	return allTableNames
}

func parseFile(content string) []oracleDbSchema {
	var plus int = 0
	var strippedString string = ""
	var splittedString []string = []string{""}
	var oracleTableData oracleDbSchema = oracleDbSchema{}
	var allOracleTableData []oracleDbSchema

	for _, line := range strings.Split(strings.TrimSuffix(content, "\n"), "\n") {
		if string(line[0]) == "+" {
			plus++
		}
		if plus == 4 {
			if string(line[0]) == "+" {
				continue
			}

			strippedString = strings.ReplaceAll(line, " ", "")
			splittedString = strings.Split(strippedString, "|")

			for i, item := range splittedString {
				switch i {
				case 0:
					continue
				case 1:
					oracleTableData.owner = item
				case 2:
					oracleTableData.tableName = item
				case 3:
					oracleTableData.columnName = item
				case 4:
					oracleTableData.dataType = item
				case 7:
					oracleTableData.dataLength, _ = strconv.Atoi(item)
				case 8:
					oracleTableData.dataPrecision, _ = strconv.Atoi(item)
				case 10:
					if item == "Y" {
						oracleTableData.nullable = true
					} else {
						oracleTableData.nullable = false
					}
				default:
					continue
				}
			}
			allOracleTableData = append(allOracleTableData, oracleTableData)

		}

	}
	log.Debug().Interface("allOracleTableData", allOracleTableData)

	return allOracleTableData
}

func openFile(filename string) string {
	dat, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Panic().Err(err).Msg("Couldn't read file")
	}

	return string(dat)
}
