package main

import (
	"context"

	"github.com/starslipay/account_mgr/account_mgr_pb"
	"github.com/zeromicro/go-zero/zrpc"
)

type AccountMgrClient struct {
	cli zrpc.Client
}

func NewAccountMgrClient(conf zrpc.RpcClientConf) *AccountMgrClient {
	conn := zrpc.MustNewClient(conf)
	return &AccountMgrClient{
		cli: conn,
	}
}

func (c *AccountMgrClient) C2CAsyncAccount(transactionId string) (*account_mgr_pb.C2CAsyncAccountRsp, error) {
	client := account_mgr_pb.NewAccountMgrClient(c.cli.Conn())
	return client.C2CAsyncAccount(context.Background(), &account_mgr_pb.C2CAsyncAccountReq{
		TransactionId: transactionId,
	})
}

func (c *AccountMgrClient) C2BAsyncAccount(transactionId string) (*account_mgr_pb.C2BAsyncAccountRsp, error) {
	client := account_mgr_pb.NewAccountMgrClient(c.cli.Conn())
	return client.C2BAsyncAccount(context.Background(), &account_mgr_pb.C2BAsyncAccountReq{
		TransactionId: transactionId,
	})
}

func (c *AccountMgrClient) B2CAsyncAccount(transactionId string) (*account_mgr_pb.B2CAsyncAccountRsp, error) {
	client := account_mgr_pb.NewAccountMgrClient(c.cli.Conn())
	return client.B2CAsyncAccount(context.Background(), &account_mgr_pb.B2CAsyncAccountReq{
		TransactionId: transactionId,
	})
}
