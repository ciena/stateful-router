package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/khagerma/stateful-experiment/protos/server"
	"github.com/khagerma/stateful-experiment/routing_service"
	"github.com/khagerma/stateful-experiment/stateful_service_implementation"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
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

	client := routing.NewRoutingService(uint32(ordinal), stateful_service.New())

	//go sendDummyRequests(client, uint32(ordinal))

	go cli(client)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println(<-sigs)
}

func sendDummyRequests(client stateful.StatefulServer, ordinal uint32) {
	for device, ctr := uint64(0), 0; true; device, ctr = (device+1)%6, ctr+1 {
		time.Sleep(time.Second * 1)
		if response, err := client.GetData(context.Background(), &stateful.GetDataRequest{Device: device}); err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("Device:", device, "Data:", string(response.Data))
		}

		if _, err := client.SetData(context.Background(), &stateful.SetDataRequest{Device: device, Data: []byte(fmt.Sprint("some string ", ordinal, " ", ctr))}); err != nil {
			fmt.Println(err)
		}
	}
}

func cli(client stateful.StatefulServer) {
	regex := regexp.MustCompile(`^(set|get) (\d+)(.*)$`)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		array := regex.FindStringSubmatch(scanner.Text())
		if array == nil {
			fmt.Println("not a valid command")
			continue
		}

		deviceId, err := strconv.ParseUint(array[2], 10, 64)
		if err != nil {
			panic(err)
		}

		if array[1] == "set" {
			if _, err := client.SetData(context.Background(), &stateful.SetDataRequest{Device: deviceId, Data: []byte(strings.TrimSpace(array[3]))}); err != nil {
				fmt.Println(">", err)
			} else {
				fmt.Println("> OK")
			}
		} else if array[1] == "get" {
			resp, err := client.GetData(context.Background(), &stateful.GetDataRequest{Device: deviceId})
			if err != nil {
				fmt.Println(">", err)
			} else {
				fmt.Println(">", string(resp.Data))
			}
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}
}
