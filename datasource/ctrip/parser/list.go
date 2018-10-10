package parser

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os/exec"
	"regexp"
	"strings"
)

const jsRe = `(?sU)(var condition =.*)</script>`
const keyRe = `{"SearchKey":"(.+)"}`

var jsReCompile = regexp.MustCompile(jsRe)
var keyReCompile = regexp.MustCompile(keyRe)

type FlightResult struct {
	Status int
}

func main() {
	key, err := GetSearchKey("SHA", "BJS", "20181010")
	if err != nil {
		panic(err)
	}
	fmt.Println(key)

	url := fmt.Sprintf("http://flights.ctrip.com/process/FlightStatus/FindByCityWithJson?"+
		"from=SHA&to=BJS&date=20181010&searchKey=%s", key)
	bytes, err := Fetch(url)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", bytes)
}

func Fetch(url string) ([]byte, error) {
	request, _ := http.NewRequest("GET", url, nil)
	request.Header.Set("Referer", "http://flights.ctrip.com/actualtime/SHA-BJS/t20181009")

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("fetch %s error: %s", url, err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(fmt.Sprintf("http: wrong status code:%d", resp.StatusCode))
	}

	return ioutil.ReadAll(resp.Body)
}

func GetSearchKey(dep, arr, date string) (string, error) {
	bytes, err := Fetch(fmt.Sprintf("http://flights.ctrip.com/actualtime/%s-%s/t%s", dep, arr, date))
	if err != nil {
		return "", err
	}
	matches := jsReCompile.FindSubmatch(bytes)

	if len(matches) < 2 {
		return "", errors.New("not match js code")
	}
	jscode := strings.Replace(string(matches[1]), "        ", "", -1)
	jscode = strings.Replace(jscode, "    ", "", -1)
	jscode = fmt.Sprintf("//jscode start\n%sconsole.log(condition);\nphantom.exit();\n//jscode end", jscode)
	fmt.Printf("%s\n", jscode)
	err = ioutil.WriteFile("jscode.js", []byte(jscode), 0644)
	if err != nil {
		return "", err
	}

	// exec phantomjs
	cmd := exec.Command("phantomjs", "jscode.js")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", err
	}
	defer stdout.Close()

	if err = cmd.Start(); err != nil {
		return "", err
	}
	opBytes, err := ioutil.ReadAll(stdout)
	if err != nil {
		return "", err
	}

	submatch := keyReCompile.FindSubmatch(opBytes)
	if len(submatch) < 2 {
		return "", errors.New("not match")
	}

	return string(submatch[1]), nil
}
