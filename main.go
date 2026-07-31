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

	c2cConsumer := NewC2CConsumer(accountMgrClient)
	c2bConsumer := NewC2BConsumer(accountMgrClient)

	pC2C := kq.MustNewQueue(c.KqConf, kq.WithHandle(func(ctx context.Context, key, value string) error {
		return c2cConsumer.Consume(ctx, []byte(value))
	}))

	pC2B := kq.MustNewQueue(c.KqConfC2B, kq.WithHandle(func(ctx context.Context, key, value string) error {
		return c2bConsumer.Consume(ctx, []byte(value))
	}))

	fmt.Printf("Starting c2c async account consumer...\n")
	go pC2C.Start()

	fmt.Printf("Starting c2b async account consumer...\n")
	pC2B.Start()
}
