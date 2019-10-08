package router

import (
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"os"
	"regexp"
	"strconv"
)

func GRPCSettings() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.KeepaliveParams(
			keepalive.ServerParameters{
				Time:    keepaliveTime,
				Timeout: keepaliveTimeout,
			}),
		grpc.KeepaliveEnforcementPolicy(
			keepalive.EnforcementPolicy{
				MinTime:             keepaliveTime / 2,
				PermitWithoutStream: true,
			}),
	}
}

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
