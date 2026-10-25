package main

import (
	"github.com/zeromicro/go-queue/kq"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	AccountMgrRpc zrpc.RpcClientConf
	KqConf        kq.KqConf
	KqConfC2B     kq.KqConf
	KqConfB2C     kq.KqConf
}
