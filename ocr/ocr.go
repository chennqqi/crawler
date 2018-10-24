package ocr

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/champkeh/crawler/config"
	_ "github.com/denisenkom/go-mssqldb"
)

const base64header string = "data:image/png;base64,"

var (
	db      *sql.DB
	codemap map[string]string
)

type codetimeEntry struct {
	code string
	time string
}

func init() {
	var err error

	// 连接 FlightData 数据库
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s", config.SqlUser, config.SqlPass, config.SqlHost,
		"FlightData")
	db, err = sql.Open("sqlserver", connstr)
	if err != nil {
		panic(err)
	}

	//初始化的时候，把所有的code都加载到内存
	query := fmt.Sprintf("select code,time from dbo.code_to_time")
	rows, err := db.Query(query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	codemap = make(map[string]string)
	var entry = codetimeEntry{}
	count := 0
	for rows.Next() {
		err := rows.Scan(&entry.code, &entry.time)
		if err != nil {
			panic(err)
		}

		codemap[entry.code] = entry.time
		count++
	}
	fmt.Printf("code-time load completed(%d)\n", count)
}

func CodeToTime(code string) (string, error) {

	// 优先查找内存中的数据缓存
	if timestr, ok := codemap[code]; ok {
		return timestr, nil
	}

	// 再检查一下数据库
	fmt.Printf("cache entry not found %s\n", code)
	query := fmt.Sprintf("select time from dbo.code_to_time where code = '%s'", code)
	var timestr string
	err := db.QueryRow(query).Scan(&timestr)
	if err != nil {
		return "", err
	}
	return timestr, nil
}

func Resolve(code string) (string, error) {
	url := ctorURL(code)
	base64str := convertToBase64(url)

	result, err := verify(base64str)
	if err != nil {
		return "", err
	}

	return result.Result, nil
}

func ctorURL(code string) string {
	return fmt.Sprintf("http://www.umetrip.com/mskyweb/graphic.do"+
		"?str=%s&width=130&height=50&front=0,0,0&back=255,255,255&size=55&xpos=0&ypos=42", code)
}

func convertToBase64(url string) string {
	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	base64str := base64.StdEncoding.EncodeToString(bytes)

	return base64header + base64str
}

func verify(base64str string) (OcrResult, error) {
	body := fmt.Sprintf("{\"base64\":\"%s\"}", base64str)
	reader := strings.NewReader(body)

	resp, err := http.Post("http://118.24.23.141:8080/base64", "application/json", reader)
	if err != nil {
		return OcrResult{}, err
	}
	defer resp.Body.Close()

	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return OcrResult{}, err
	}

	//fmt.Printf("%d: %s\n", resp.StatusCode, bytes)
	var result OcrResult
	err = json.Unmarshal(bytes, &result)

	return result, err
}

type OcrResult struct {
	Result  string `json:"result"`
	Version string `json:"version"`
}
