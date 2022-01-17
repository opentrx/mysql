// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/opentrx/seata-golang/v2/pkg/apis"
	"github.com/opentrx/seata-golang/v2/pkg/client/base/exception"
	"github.com/opentrx/seata-golang/v2/pkg/client/config"
	"github.com/opentrx/seata-golang/v2/pkg/client/rm"
	"github.com/opentrx/seata-golang/v2/pkg/util/log"
	"github.com/pkg/errors"
)

type mysqlTx struct {
	mc *mysqlConn
}

func (tx *mysqlTx) Commit() (err error) {
	defer func() {
		if tx.mc != nil {
			tx.mc.ctx = nil
		}
		tx.mc = nil
	}()

	if tx.mc == nil || tx.mc.closed.IsSet() {
		return ErrInvalidConn
	}

	if tx.mc.ctx != nil {
		branchID, err := tx.register()
		if err != nil {
			rollBackErr := tx.mc.exec("ROLLBACK")
			if rollBackErr != nil {
				log.Error(rollBackErr)
			}
			return err
		}
		tx.mc.ctx.branchID = branchID

		if len(tx.mc.ctx.sqlUndoItemsBuffer) > 0 {
			err = GetUndoLogManager().FlushUndoLogs(tx.mc)
			if err != nil {
				reportErr := tx.report(false)
				if reportErr != nil {
					return reportErr
				}
				return err
			}
			err = tx.mc.exec("COMMIT")
			if err != nil {
				reportErr := tx.report(false)
				if reportErr != nil {
					return reportErr
				}
				return err
			}
		} else {
			err = tx.mc.exec("COMMIT")
			return err
		}
	} else {
		err = tx.mc.exec("COMMIT")
		return err
	}
	return
}

func (tx *mysqlTx) Rollback() (err error) {
	defer func() {
		if tx.mc != nil {
			tx.mc.ctx = nil
		}
		tx.mc = nil
	}()

	if tx.mc == nil || tx.mc.closed.IsSet() {
		return ErrInvalidConn
	}
	err = tx.mc.exec("ROLLBACK")

	if tx.mc.ctx != nil {
		branchID, err := tx.register()
		if err != nil {
			return err
		}
		tx.mc.ctx.branchID = branchID
		tx.report(false)
	}
	return
}

func (tx *mysqlTx) register() (int64, error) {
	var branchID int64
	var err error
	for retryCount := 0; retryCount < config.GetATConfig().LockRetryTimes; retryCount++ {
		itemsBuffer := tx.mc.ctx.sqlUndoItemsBuffer
		skipCheckLock := true
		var applicationData []byte
		for _, undoLog := range itemsBuffer {
			if undoLog.BeforeImage != nil && len(undoLog.BeforeImage.Rows) != 0 {
				skipCheckLock = false
				break
			}
		}
		if skipCheckLock {
			applicationDataMap := map[string]bool{
				"skipCheckLock": skipCheckLock,
			}
			applicationData, _ = json.Marshal(applicationDataMap)
		}
		branchID, err = rm.GetResourceManager().BranchRegister(context.Background(),
			tx.mc.ctx.xid, tx.mc.cfg.DBName, apis.AT, applicationData,
			strings.Join(tx.mc.ctx.lockKeys, ";"))
		if err == nil {
			break
		}
		log.Errorf("branch register err: %v", err)
		var tex *exception.TransactionException
		if errors.As(err, &tex) {
			if tex.Code == apis.GlobalTransactionNotExist {
				break
			}
		}
		time.Sleep(config.GetATConfig().LockRetryInterval)
	}
	return branchID, err
}

func (tx *mysqlTx) report(commitDone bool) error {
	retry := config.GetATConfig().LockRetryTimes
	for retry > 0 {
		var err error
		if commitDone {
			err = rm.GetResourceManager().BranchReport(context.Background(),
				tx.mc.ctx.xid, tx.mc.ctx.branchID, apis.AT, apis.PhaseOneDone, nil)
		} else {
			err = rm.GetResourceManager().BranchReport(context.Background(),
				tx.mc.ctx.xid, tx.mc.ctx.branchID, apis.AT, apis.PhaseOneFailed, nil)
		}
		if err != nil {
			log.Errorf("Failed to report [%d/%s] commit done [%t] Retry Countdown: %d",
				tx.mc.ctx.branchID, tx.mc.ctx.xid, commitDone, retry)
		}
		retry = retry - 1
		if retry == 0 {
			return errors.WithMessagef(err, "Failed to report branch status %t", commitDone)
		}
	}
	return nil
}
