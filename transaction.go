// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"strings"
	"time"
)

import (
	"github.com/pkg/errors"
	"github.com/transaction-wg/seata-golang/pkg/base/meta"
	"github.com/transaction-wg/seata-golang/pkg/client/config"
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
			err = tx.Rollback()
			return errors.WithStack(err)
		}
		tx.mc.ctx.branchID = branchID

		if len(tx.mc.ctx.sqlUndoItemsBuffer) > 0 {
			err = GetUndoLogManager().FlushUndoLogs(tx.mc)
			if err != nil {
				err1 := tx.report(false)
				if err1 != nil {
					return errors.WithStack(err1)
				}
				return errors.WithStack(err)
			}
			err = tx.mc.exec("COMMIT")
			if err != nil {
				err1 := tx.report(false)
				if err1 != nil {
					return errors.WithStack(err1)
				}
				return errors.WithStack(err)
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
			return errors.WithStack(err)
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
		branchID, err = dataSourceManager.BranchRegister(meta.BranchTypeAT, tx.mc.cfg.DBName, "", tx.mc.ctx.xid,
			nil, strings.Join(tx.mc.ctx.lockKeys, ";"))
		if err == nil {
			break
		}
		errLog.Print("branch register err: %v", err)
		var tex *meta.TransactionException
		if errors.As(err, &tex) {
			if tex.Code == meta.TransactionExceptionCodeGlobalTransactionNotExist {
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
			err = dataSourceManager.BranchReport(meta.BranchTypeAT, tx.mc.ctx.xid, tx.mc.ctx.branchID,
				meta.BranchStatusPhaseoneDone, nil)
		} else {
			err = dataSourceManager.BranchReport(meta.BranchTypeAT, tx.mc.ctx.xid, tx.mc.ctx.branchID,
				meta.BranchStatusPhaseoneFailed, nil)
		}
		if err != nil {
			errLog.Print("Failed to report [%d/%s] commit done [%t] Retry Countdown: %d",
				tx.mc.ctx.branchID, tx.mc.ctx.xid, commitDone, retry)
		}
		retry = retry - 1
		if retry == 0 {
			return errors.WithMessagef(err, "Failed to report branch status %t", commitDone)
		}
	}
	return nil
}
