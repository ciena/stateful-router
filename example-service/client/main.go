package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/kent-h/stateful-router/example-service/protos/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"os"
	"regexp"
	"strings"
	"time"
)

func main() {
	cc, err := grpc.Dial("stateful-experiment-api.voltha.svc.cluster.local:2350",
		grpc.WithBlock(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Second * 10,
			Timeout:             time.Second * 5,
			PermitWithoutStream: true,
		}),
		grpc.WithInsecure(),
		grpc.WithBackoffConfig(grpc.BackoffConfig{MaxDelay: time.Second * 5}))
	if err != nil {
		panic(err)
	}
	fmt.Println("Connected")

	client := stateful.NewStatefulClient(cc)

	cli(client)
}

func cli(client stateful.StatefulClient) {
	regex := regexp.MustCompile(`^(set|get) ([^ ]+)(.*)$`)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		array := regex.FindStringSubmatch(scanner.Text())
		if array == nil {
			fmt.Println("not a valid command")
			continue
		}

		deviceId := array[2]

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
