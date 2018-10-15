package parser

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os/exec"
	"regexp"
	"strings"

	"encoding/json"

	"bytes"

	"os"

	"log"

	"github.com/champkeh/crawler/fetcher"
	"github.com/champkeh/crawler/types"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

const jscodeRe = `(?sU)(var condition =.*)</script>`
const keyRe = `{"SearchKey":"(.+)"}`

var jsReCompile = regexp.MustCompile(jscodeRe)
var keyReCompile = regexp.MustCompile(keyRe)

// date format: 2018-10-10
func GetSearchKey(dep, arr, date string, rateLimiter types.RateLimiter) (string, error) {
	date = strings.Replace(date, "-", "", -1)
	fetchURL := fmt.Sprintf("http://flights.ctrip.com/actualtime/%s-%s/t%s", dep, arr, date)
	referer := "http://flights.ctrip.com/"

	contents, err := fetcher.Fetch(fetchURL, referer, rateLimiter)
	if err != nil {
		return "", err
	}
	return parseSearchKey(contents, dep+arr+date+"jscode.js")
}

func parseSearchKey(contents []byte, filename string) (string, error) {
	matches := jsReCompile.FindSubmatch(contents)

	if len(matches) < 2 {
		return "", errors.New(fmt.Sprintf("parse search key error: not match js code"))
	}

	buffer := bytes.NewBuffer(matches[1])
	buffer.WriteString(";console.log(condition);")
	buffer.WriteString("phantom.exit();")

	err := ioutil.WriteFile(filename, buffer.Bytes(), 0644)
	if err != nil {
		return "", errors.New(fmt.Sprintf("write jscode file error: %s", err))
	}
	defer os.Remove(filename)

	// exec phantomjs
	cmd := exec.Command("phantomjs", filename)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", errors.New(fmt.Sprintf("cmd stdout pipe error: %s", err))
	}
	defer stdout.Close()

	if err = cmd.Start(); err != nil {
		return "", errors.New(fmt.Sprintf("cmd run fail: %s", err))
	}

	cmdResult, err := ioutil.ReadAll(stdout)
	if err != nil {
		return "", errors.New(fmt.Sprintf("read cmd output fail: %s", err))
	}
	cmd.Wait()

	submatch := keyReCompile.FindSubmatch(cmdResult)
	if len(submatch) < 2 {
		return "", errors.New(fmt.Sprintf("parse search key error: %s not match key", cmdResult))
	}

	return string(submatch[1]), nil
}

// date format: 2018-10-10
func GetListResult(dep, arr, date, key string, rateLimiter types.RateLimiter) (types.ParseResult, error) {

	fetchURL := fmt.Sprintf("http://flights.ctrip.com/process/FlightStatus/FindByCityWithJson?"+
		"from=%s&to=%s&date=%s&searchKey=%s",
		dep, arr, strings.Replace(date, "-", "", -1), key)
	referer := "http://flights.ctrip.com/"

	contents, err := fetcher.Fetch(fetchURL, referer, rateLimiter)
	if err != nil {
		return types.ParseResult{}, err
	}

	parseResult, err := parseListResult(contents)

	// attach request
	parseResult.Request = types.Request{
		RawParam: types.Param{
			Date: date,
			Dep:  dep,
			Arr:  arr,
		},
	}

	return parseResult, err
}
func parseListResult(contents []byte) (types.ParseResult, error) {
	result := SearchResult{}

	reader := transform.NewReader(bytes.NewReader(contents), simplifiedchinese.GBK.NewDecoder())
	err := json.NewDecoder(reader).Decode(&result)
	if err != nil {
		return types.ParseResult{}, err
	}

	if result.Status != 200 {
		return types.ParseResult{}, nil
	}
	parseResult := types.ParseResult{}
	for _, item := range result.List {
		parseResult.Items = append(parseResult.Items, item)
	}
	return parseResult, nil
}

func printMatch(matches [][]byte) {
	for _, c := range matches {
		log.Printf("\n%s\n", c)
	}
}

type FlightListData struct {
	AAirportCode string
	AAirportName string
	ACityCode    string
	ACityName    string
	ATerminal    string
	ATimeZone    string

	DAirportCode string
	DAirportName string
	DCityCode    string
	DCityName    string
	DTerminal    string
	DTimeZone    string

	DDate            string
	FlightNo         string
	CompanyShortName string
	Status           string
	PlanDDateTime    string
	PlanADateTime    string
}

type SearchResult struct {
	Status int
	List   []FlightListData
}
