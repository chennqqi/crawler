package persist

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/champkeh/crawler/config"
	_ "github.com/denisenkom/go-mssqldb"
)

func TestEqual(t *testing.T) {
	// 连接到 FlightData 数据库
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s&connection+timeout=10",
		config.SqlUser, config.SqlPass, config.SqlAddr, "FlightData")
	db, err := sql.Open("sqlserver", connstr)
	if err != nil {
		panic(err)
	}
	// 获取数据库中该航班的最新状态并进行比较
	var dbFlightState dbFlight
	db.QueryRow(fmt.Sprintf(
		"select top 1 flightNo,date,depCode,arrCode,flightState,code1,code2,code3,code4,preFlightNo,preFlightState from [dbo].[RealTime] "+
			"where flightNo='%s' and date='%s' and depCode='%s' and arrCode='%s' "+
			"order by createAt desc",
		"MF8185", "2018-09-20", "FOC", "CSX")).Scan(
		&dbFlightState.FlightNo,
		&dbFlightState.Date,
		&dbFlightState.DepCode,
		&dbFlightState.ArrCode,
		&dbFlightState.FlightState,
		&dbFlightState.Code1,
		&dbFlightState.Code2,
		&dbFlightState.Code3,
		&dbFlightState.Code4,
		&dbFlightState.PreFlightNo,
		&dbFlightState.PreFlightState)
	fmt.Println(dbFlightState)
}
