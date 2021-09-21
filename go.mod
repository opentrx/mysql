module github.com/opentrx/mysql/v2

go 1.10

require (
	github.com/go-playground/assert/v2 v2.0.1
	github.com/golang/protobuf v1.4.3
	github.com/google/go-cmp v0.5.2
	github.com/opentrx/seata-golang/v2 v2.0.5
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/pingcap/parser v0.0.0-20200424075042-8222d8b724a4
	github.com/pkg/errors v0.9.1
	golang.org/x/text v0.3.5 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	vimagination.zapto.org/byteio v0.0.0-20200222190125-d27cba0f0b10
	vimagination.zapto.org/memio v1.0.0 // indirect
)

replace github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.4

replace go.etcd.io/bbolt => github.com/coreos/bbolt v1.3.4
