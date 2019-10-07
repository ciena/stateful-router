package router

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
)

func MustParseOrdinal(ordinalStr string) uint32 {
	if ordinalStr == "" {
		hostname, err := os.Hostname()
		if err != nil {
			panic(fmt.Errorf("unable to read hostname, and ordinal not set: %s", err))
		}

		ordinalStr = regexp.MustCompile(`\d+$`).FindString(hostname)
		if ordinalStr == "" {
			panic("hostname does not have the form .*\\d+$")
		}
	}

	ordinal, err := strconv.ParseInt(ordinalStr, 10, 32)
	if err != nil {
		panic(fmt.Errorf("unable to parse ordinal: %s", err))
	}
	return uint32(ordinal)
}
