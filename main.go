package main

import (
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"strconv"

	// "github.com/urfave/cli/v2"
	"io/ioutil"
	"strings"
)

var ora2ChConversion = map[string]string{
	"VARCHAR2": "string",
	"NUMBER": "decimal",
	"CHAR": "string",
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

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	var fileContent string = openFile("output_example")
	var data []oracleDbSchema
	log.Debug().Str("file content", fileContent)

	if fileContent == ""{
		log.Error().Msg("No file found...")
		os.Exit(2)
	}

	data = parseFile(fileContent)

	generateClickhouseQuery(data)

}

func generateClickhouseQuery(oracleDbSchemas []oracleDbSchema) {

	var sqlTablePrefix string = "ora_"
	var sqlColumns string
	var sqlHead string
	var sqlFoot string
	var sqlDropTable string

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
		sqlQuery := sqlDropTable + "\n" + sqlHead + sqlColumns + sqlFoot + "\n\n"
		fmt.Println(sqlQuery)
		sqlHead = ""
		sqlColumns = ""
		sqlFoot = ""
	}

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


func parseFile(content string) []oracleDbSchema{
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
					if item == "Y"{
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
func openFile(filename string) string{

	dat, err := ioutil.ReadFile(filename)
	check(err)

	return string(dat)
}

func check(e error) {
	if e != nil {
		log.Err(e)
	}
}