module github.com/kent-h/stateful-router/example-service/dummy-db

go 1.13

require (
	github.com/kent-h/stateful-router v0.0.0-20191017140925-4f0b3c9f1287 // indirect
	github.com/kent-h/stateful-router/example-service/protos v0.0.0-00010101000000-000000000000 // indirect
)

replace github.com/kent-h/stateful-router => ../..

replace github.com/kent-h/stateful-router/example-service/protos => ../protos
