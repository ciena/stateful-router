module github.com/kent-h/stateful-router/example-service

go 1.13

require (
	github.com/golang/protobuf v1.3.2
	github.com/kent-h/stateful-router v0.0.0-20191017140925-4f0b3c9f1287
	google.golang.org/grpc v1.24.0
)

replace github.com/kent-h/stateful-router => ./..
