module github.com/kent-h/stateful-router/example-service/main

go 1.13

require (
	github.com/kent-h/stateful-router/example-service/protos v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.24.0
)

replace github.com/kent-h/stateful-router => ../..

replace github.com/kent-h/stateful-router/example-service/protos => ../protos
