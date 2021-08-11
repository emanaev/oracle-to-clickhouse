package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

var flagFile string
var flagDSN string
var flagDebug bool

// TODO: add more data types and find a solution for the precision problem
var ora2ChConversion = map[string]string{
	"VARCHAR2": "string",
	"NUMBER":   "decimal",
	"CHAR":     "string",
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
	flag.StringVar(&flagFile, "file", "", "file path an name")
	flag.StringVar(&flagDSN, "dsn", "", "dsn name of odbc service")
	flag.BoolVar(&flagDebug, "debug", false, "debug mode")
}

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	var data []oracleDbSchema

	flag.Parse()

	if flagDebug {
		flagFile = "output_example"
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	if flagFile != "" && flagDSN != "" {
		log.Panic().Msg("Provided too many flags, only one is allowed")
	} else if flagFile != "" && flagDSN == "" {
		data = connectByFile(flagFile)
	} else if flagDSN != "" && flagFile == "" {
		data = connectByODBC(flagDSN)
	} else {
		log.Panic().Msg("No flags were provided, need at least one")
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

	//Open Connection. Provide DSN, Username and Password
	db, err := sql.Open("odbc", fmt.Sprintf("DSN=%v", dsn))
	if err != nil {
		log.Panic().Err(err).Msg("Couldn't connect to database")
	} else {
		log.Debug().Msg("Connection to DB successful")
	}

	//Provide the Query to execute
	rows, err := db.Query("SELECT * from all_tab_columns")

	if err != nil {
		log.Panic().Err(err).Msg("Unable to query")
	}

	//Parse the Result set
	for rows.Next() {
		err = rows.Scan(&queryResult.owner, &queryResult.tableName, &queryResult.columnName, &queryResult.dataType, nil, nil, &queryResult.dataLength, &queryResult.dataPrecision, nil, &queryResult.nullable)
		if err != nil {
			log.Error().Err(err).Msg("Error while parsing result")
		}
		allQueryResult = append(allQueryResult, queryResult)
	}

	//Close the connection
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
	f, err := os.Create("clickhouse_query.sql")
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

		for _, item := range oracleDbSchemas {

			log.Debug().Str("oracleDbSchemas", item.tableName)
			sqlDropTable = fmt.Sprintf("DROP TABLE IF EXIST %v;", allTableName.tableName)
			sqlHead = fmt.Sprintf("CREATE TABLE %v%v (", sqlTablePrefix, allTableName.tableName)
			sqlFoot = fmt.Sprintf(") ENGINE = ODBC(dsnName, %v, %v);", item.owner, allTableName.tableName)
			if item.tableName == allTableName.tableName {
				sqlColumns += fmt.Sprintf("`%v` %v,", item.columnName, ora2ChConversion[item.dataType])
				log.Debug().Str("sqlHead", sqlHead).Str("sqlFoot", sqlFoot)
			}
		}
		//cut off trailing comma in sql query
		sqlColumns = sqlColumns[:len(sqlColumns)-1]
		sqlQuery = sqlDropTable + "\n" + sqlHead + sqlColumns + sqlFoot + "\n\n"
		log.Debug().Str("sqlQuery", sqlQuery)
		sqlHead = ""
		sqlColumns = ""
		sqlFoot = ""
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
