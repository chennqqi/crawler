package ocr

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

const base64header string = "data:image/png;base64,"

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
