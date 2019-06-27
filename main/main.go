package main

import (
	"context"
	"fmt"
	"github.com/khagerma/stateful-experiment"
	"github.com/khagerma/stateful-experiment/protos/server"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"syscall"
	"time"
)

func main() {

	ordinalEnv, have := os.LookupEnv("ORDINAL")
	if !have {
		hostname, err := os.Hostname()
		if err != nil {
			panic(err)
		}
		regex := regexp.MustCompile(`\d+$`)
		array := regex.FindString(hostname)
		if array == "" {
			panic("NAMESPACE env var is not of the form *.-\\d")
		}
		ordinalEnv = array
	}

	ordinal, err := strconv.ParseInt(ordinalEnv, 10, 32)
	if err != nil {
		panic(err)
	}

	fmt.Println("ordinal:", ordinal)

	client := ha_service.NewRoutingService(uint32(ordinal))

	client = client

	go sendDummyRequests(client, uint32(ordinal))

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println(<-sigs)
}

func sendDummyRequests(client stateful.StatefulServer, ordinal uint32) {
	for {
		time.Sleep(time.Second * 1)

		if response, err := client.GetData(context.Background(), &stateful.GetDataRequest{Device: 0}); err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(string(response.Data))
		}

		if _, err := client.SetData(context.Background(), &stateful.SetDataRequest{Device: 0, Data: []byte(fmt.Sprint("some string ", ordinal))}); err != nil {
			fmt.Println(err)
		}
	}
}
