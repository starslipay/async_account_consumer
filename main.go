package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/zeromicro/go-queue/kq"
	"github.com/zeromicro/go-zero/core/conf"
)

var configFile = flag.String("f", "etc/config.yaml", "the config file")

func main() {
	flag.Parse()

	var c Config
	conf.MustLoad(*configFile, &c)

	accountMgrClient := NewAccountMgrClient(c.AccountMgrRpc)
	consumer := NewC2CConsumer(accountMgrClient)

	p := kq.MustNewQueue(c.KqConf, kq.WithHandle(func(ctx context.Context, key, value string) error {
		return consumer.Consume(ctx, []byte(value))
	}))

	fmt.Printf("Starting c2c async account consumer...\n")
	p.Start()
}
