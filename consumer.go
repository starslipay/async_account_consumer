package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

type PendingC2CTransferMessage struct {
	TransactionId string `json:"transaction_id"`
}

type PendingC2BTransferMessage struct {
	TransactionId string `json:"transaction_id"`
}

type PendingB2CTransferMessage struct {
	TransactionId string `json:"transaction_id"`
}

type C2CConsumer struct {
	accountMgrClient *AccountMgrClient
	logger           logx.Logger
}

type C2BConsumer struct {
	accountMgrClient *AccountMgrClient
	logger           logx.Logger
}

func NewC2CConsumer(accountMgrClient *AccountMgrClient) *C2CConsumer {
	return &C2CConsumer{
		accountMgrClient: accountMgrClient,
		logger:           logx.WithContext(context.Background()),
	}
}

func NewC2BConsumer(accountMgrClient *AccountMgrClient) *C2BConsumer {
	return &C2BConsumer{
		accountMgrClient: accountMgrClient,
		logger:           logx.WithContext(context.Background()),
	}
}

type B2CConsumer struct {
	accountMgrClient *AccountMgrClient
	logger           logx.Logger
}

func NewB2CConsumer(accountMgrClient *AccountMgrClient) *B2CConsumer {
	return &B2CConsumer{
		accountMgrClient: accountMgrClient,
		logger:           logx.WithContext(context.Background()),
	}
}

func (c *C2CConsumer) Consume(ctx context.Context, message []byte) error {
	var msg PendingC2CTransferMessage
	if err := json.Unmarshal(message, &msg); err != nil {
		c.logger.Errorf("unmarshal message failed: %v", err)
		return err
	}

	c.logger.Infof("received c2c transfer message: transaction_id=%s",
		msg.TransactionId)

	maxRetries := 3
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		_, err := c.accountMgrClient.C2CAsyncAccount(msg.TransactionId)
		if err != nil {
			lastErr = err
			c.logger.Errorf("call C2CAsyncAccount failed (retry %d/%d): %v", i+1, maxRetries, err)
			time.Sleep(time.Duration(i+1) * time.Second)
			continue
		}

		c.logger.Infof("c2c async account success: transaction_id=%s", msg.TransactionId)
		return nil
	}

	c.logger.Errorf("call C2CAsyncAccount failed after %d retries: %v", maxRetries, lastErr)
	return fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}

func (c *C2BConsumer) Consume(ctx context.Context, message []byte) error {
	var msg PendingC2BTransferMessage
	if err := json.Unmarshal(message, &msg); err != nil {
		c.logger.Errorf("unmarshal message failed: %v", err)
		return err
	}

	c.logger.Infof("received c2b transfer message: transaction_id=%s",
		msg.TransactionId)

	maxRetries := 3
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		_, err := c.accountMgrClient.C2BAsyncAccount(msg.TransactionId)
		if err != nil {
			lastErr = err
			c.logger.Errorf("call C2BAsyncAccount failed (retry %d/%d): %v", i+1, maxRetries, err)
			time.Sleep(time.Duration(i+1) * time.Second)
			continue
		}

		c.logger.Infof("c2b async account success: transaction_id=%s", msg.TransactionId)
		return nil
	}

	c.logger.Errorf("call C2BAsyncAccount failed after %d retries: %v", maxRetries, lastErr)
	return fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}

func (c *B2CConsumer) Consume(ctx context.Context, message []byte) error {
	var msg PendingB2CTransferMessage
	if err := json.Unmarshal(message, &msg); err != nil {
		c.logger.Errorf("unmarshal message failed: %v", err)
		return err
	}

	c.logger.Infof("received b2c transfer message: transaction_id=%s",
		msg.TransactionId)

	maxRetries := 3
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		_, err := c.accountMgrClient.B2CAsyncAccount(msg.TransactionId)
		if err != nil {
			lastErr = err
			c.logger.Errorf("call B2CAsyncAccount failed (retry %d/%d): %v", i+1, maxRetries, err)
			time.Sleep(time.Duration(i+1) * time.Second)
			continue
		}

		c.logger.Infof("b2c async account success: transaction_id=%s", msg.TransactionId)
		return nil
	}

	c.logger.Errorf("call B2CAsyncAccount failed after %d retries: %v", maxRetries, lastErr)
	return fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}
