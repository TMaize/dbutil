package test

import (
	"testing"

	"github.com/TMaize/dbutil/pkg"
)

func TestCamelCase2UnderScoreCase(t *testing.T) {
	testCases := []struct {
		input  string
		output string
	}{
		{"IDCard", "id_card"},
		{"IdCard", "id_card"},
		{"MediaID", "media_id"},
		{"MediaId", "media_id"},
		{"AA", "aa"},
		{"Aa", "aa"},
		{"ABc", "a_bc"},
		{"aABC", "a_abc"},
	}

	for _, tc := range testCases {
		output := pkg.CamelCase2UnderScoreCase(tc.input)
		if output != tc.output {
			t.Fatalf(`"%s" => "%s" want "%s"`, tc.input, output, tc.output)
		}
	}
}
