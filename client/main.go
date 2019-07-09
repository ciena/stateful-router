package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/khagerma/stateful-experiment/protos/server"
	"google.golang.org/grpc"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func main() {
	cc, err := grpc.Dial("stateful-experiment-api.voltha.svc.cluster.local:2350", grpc.WithBlock(), grpc.WithInsecure(), grpc.WithBackoffConfig(grpc.BackoffConfig{MaxDelay: time.Second * 5}))
	if err != nil {
		panic(err)
	}
	fmt.Println("Connected")

	client := stateful.NewStatefulClient(cc)

	cli(client)
}

func cli(client stateful.StatefulClient) {
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
			client.SetData(context.Background(), &stateful.SetDataRequest{Device: deviceId, Data: []byte(strings.TrimSpace(array[3]))})
			fmt.Println("> OK")
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
