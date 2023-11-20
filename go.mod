module github.com/cubefs/inodedb

go 1.16

replace github.com/tecbot/gorocksdb v0.0.0-20191217155057-f0fad39f321c => github.com/Cloudstriff/gorocksdb v0.0.0-20230529103218-db8e5fd20894

require (
	github.com/cubefs/cubefs v1.5.2-0.20231115110837-5ab518b3598e
	github.com/gogo/protobuf v1.3.2
	github.com/golang/mock v1.6.0
	github.com/google/uuid v1.3.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/prometheus/client_golang v1.16.0
	github.com/stretchr/testify v1.8.3
	github.com/tecbot/gorocksdb v0.0.0-20191217155057-f0fad39f321c
	go.etcd.io/etcd/raft/v3 v3.5.8
	golang.org/x/sync v0.2.0
	google.golang.org/grpc v1.57.0
	google.golang.org/protobuf v1.31.0
)
