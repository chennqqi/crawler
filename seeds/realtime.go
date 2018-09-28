package seeds

import (
	"database/sql"
	"fmt"

	"time"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/umetrip/parser"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/labstack/gommon/log"
)

// 从 RealTime 表中拉取数据
func PullLatestFlight(container chan types.Request, launch bool) error {
	// 打开数据库连接
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s&connection+timeout=60",
		config.SqlUser, config.SqlPass, config.SqlAddr, "FlightData")
	db, err := sql.Open("sqlserver", connstr)
	if err != nil {
		return err
	}

	// 计算未来2小时的时间边界范围
	var now = time.Now().Format("2006-01-02 15:04:05")
	var end = time.Now().Add(2 * time.Hour).Format("2006-01-02 15:04:05")

	query := ""
	if launch {
		// 启动时，会加载当天未完成的航班
		query = fmt.Sprintf("select distinct date,flightNo from [dbo].[RealTime] "+
			"where depPlanTime <= '%s' "+
			"and flightState not in ('到达','取消','备降','返航','暂无')", end)
	} else {
		// 非启动时，尽加载未来2小时的航班
		query = fmt.Sprintf("select distinct date,flightNo from [dbo].[RealTime] "+
			"where depPlanTime >= '%s' "+
			"and depPlanTime <= '%s' "+
			"and flightState not in ('到达','取消','备降','返航','暂无')", now, end)
	}

	go func() {
		fmt.Printf("\n>>\t#%s# %q\n", time.Now().Format("2006-01-02 15:04:05"), query)
		rows, err := db.Query(query)
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		var flight Flight
		for rows.Next() {
			err := rows.Scan(&flight.FlightDate, &flight.FlightNo)
			if err != nil {
				log.Errorf("scan error: %v", err)
				continue
			}

			// 详情页url:http://www.umetrip.com/mskyweb/fs/fc.do?flightNo=MU3924&date=2018-09-13
			url := fmt.Sprintf("http://www.umetrip.com/mskyweb/fs/fc.do?flightNo=%s&date=%s",
				flight.FlightNo, flight.FlightDate)

			container <- types.Request{
				Url:        url,
				ParserFunc: parser.ParseDetail,
				RawParam: types.Param{
					Date: flight.FlightDate,
					Fno:  flight.FlightNo,
				},
				FetchCount: 0,
			}
		}
	}()

	return nil
}
