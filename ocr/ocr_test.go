package ocr

import (
	"fmt"
	"testing"
)

func TestResolve(t *testing.T) {
	cases := []struct {
		code     string
		expected string
	}{
		{code: "Q0tk1boRIwfM5bqGv9/5Lg==", expected: "17:01"},
		{code: "+524MNKi71fd4iqtfQIqzQ==", expected: "19:19"},
		{code: "+Cs1hpLIK+1gwwLuKs6pNg==", expected: "19:02"},
		{code: "4prfZAj6dGVseMSExTzV2w==", expected: "16:33"},
		{code: "crMXfzDOJjpnnyNBN/oB/w==", expected: "18:39"},
		{code: "ElCkY676CuM2pqzpUrzO/g==", expected: "08:18"},
		{code: "N8VYdy51OrhOn0CBgY463A==", expected: "15:14"},
		{code: "MBgPVSymkaQVEb8xVU5eUA==", expected: "09:51"},
		{code: "qGdBJySx+vtAvN77aFTNSA==", expected: "13:39"},
	}

	for _, c := range cases {
		actual, err := Resolve(c.code)
		if err != nil {
			t.Errorf("resolve %s error: %s", c.code, err)
		}
		if actual != c.expected {
			t.Errorf("resolve %s got %s; expected %s", c.code, actual, c.expected)
		}
	}
}

func TestCodeToTime(t *testing.T) {
	s, err := CodeToTime("OCNl2Ll92Dd8W89/tG4Q==")
	if err != nil {
		t.Errorf("convert fail: %v", err)
	}

	fmt.Printf("s = %q\n", s)
}
