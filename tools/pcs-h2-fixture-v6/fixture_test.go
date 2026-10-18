package main

import (
	"testing"
	"time"
)

func TestUploadDelayEnumeration(t *testing.T) {
	for _, test := range []struct {
		input string
		want  time.Duration
		ok    bool
	}{
		{"0", 0, true}, {"5", 5 * time.Second, true},
		{"", 0, false}, {"1", 0, false}, {"4", 0, false}, {"6", 0, false},
		{"-1", 0, false}, {"-0", 0, false}, {"+5", 0, false},
		{"05", 0, false}, {"5.0", 0, false}, {" 5", 0, false},
		{"5 ", 0, false}, {"5s", 0, false}, {"NaN", 0, false},
	} {
		t.Run(test.input, func(t *testing.T) {
			got, err := parseUploadDelay(test.input)
			if (err == nil) != test.ok || (test.ok && got != test.want) {
				t.Fatalf("input %q: duration %v error %v; want duration %v valid %v", test.input, got, err, test.want, test.ok)
			}
		})
	}
}
