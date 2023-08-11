module github.com/cubefs/inodedb

go 1.16

replace github.com/tecbot/gorocksdb v0.0.0-20191217155057-f0fad39f321c => github.com/Cloudstriff/gorocksdb v0.0.0-20230529103218-db8e5fd20894

require (
	github.com/cubefs/cubefs v0.0.0-20230620070032-24e766dd7b25
	github.com/google/uuid v1.3.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/jacobsa/daemonize v0.0.0-20160101105449-e460293e890f
	github.com/prometheus/client_golang v1.16.0
	github.com/stretchr/testify v1.8.3
	github.com/tecbot/gorocksdb v0.0.0-20191217155057-f0fad39f321c
	golang.org/x/time v0.3.0
	google.golang.org/grpc v1.57.0
	google.golang.org/protobuf v1.31.0
)
