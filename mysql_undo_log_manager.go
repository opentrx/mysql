package mysql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"time"
)

import (
	"github.com/pkg/errors"
)

const (
	DeleteUndoLogSql         = "DELETE FROM undo_log WHERE xid = ? and branch_id = ?"
	DeleteUndoLogByCreateSql = "DELETE FROM undo_log WHERE log_created <= ? LIMIT ?"
	InsertUndoLogSql         = `INSERT INTO undo_log (branch_id, xid, context, rollback_info, log_status, log_created, 
		log_modified) VALUES (?, ?, ?, ?, ?, now(), now())`
	SelectUndoLogSql = `SELECT branch_id, xid, context, rollback_info, log_status FROM undo_log 
        WHERE xid = ? AND branch_id = ? FOR UPDATE`
)

type State byte

const (
	Normal State = iota
	GlobalFinished
)

func (state State) String() string {
	switch state {
	case Normal:
		return "Normal"
	case GlobalFinished:
		return "GlobalFinished"
	default:
		return fmt.Sprintf("%d", state)
	}
}

type MysqlUndoLogManager struct {
}

func GetUndoLogManager() MysqlUndoLogManager {
	return MysqlUndoLogManager{}
}

func (manager MysqlUndoLogManager) FlushUndoLogs(conn *mysqlConn) error {
	defer func() {
		if err := recover(); err != nil {
			errLog.Print(err)
		}
	}()
	ctx := conn.ctx
	xid := ctx.xid
	branchID := ctx.branchID

	branchUndoLog := &branchUndoLog{
		Xid:         xid,
		BranchID:    branchID,
		SqlUndoLogs: ctx.sqlUndoItemsBuffer,
	}

	parser := GetUndoLogParser()
	undoLogContent := parser.Encode(branchUndoLog)
	fmt.Printf("Flushing UNDO LOG: %s", string(undoLogContent))

	return manager.insertUndoLogWithNormal(conn, xid, branchID, buildContext(parser.GetName()), undoLogContent)
}

func (manager MysqlUndoLogManager) Undo(conn *mysqlConn, xid string, branchID int64, resourceID string) error {
	tx, err := conn.Begin()
	if err != nil {
		return err
	}

	args := []driver.Value{xid, branchID}
	rows, err := conn.prepareQuery(SelectUndoLogSql, args)
	if err != nil {
		return err
	}

	exists := false

	undoLogs := make([]*branchUndoLog, 0)

	var branchID2 sql.NullInt64
	var xid2, context sql.NullString
	var rollbackInfo sql.RawBytes
	var state sql.NullInt32

	vals := make([]driver.Value, 5)
	dest := []interface{}{&branchID, &xid, &context, &rollbackInfo, &state}

	for {
		err := rows.Next(vals)
		if err != nil {
			break
		}

		for i, sv := range vals {
			err := convertAssignRows(dest[i], sv)
			if err != nil {
				return fmt.Errorf(`sql: Scan error on column index %d, name %q: %v`, i, rows.Columns()[i], err)
			}
		}

		exists = true

		if State(state.Int32) != Normal {
			fmt.Printf("xid %s branch %d, ignore %s undo_log", xid2, branchID2, State(state.Int32).String())
			return nil
		}

		//serializer := getSerializer(context)
		parser := GetUndoLogParser()
		branchUndoLog := parser.Decode(rollbackInfo)
		undoLogs = append(undoLogs, branchUndoLog)
	}
	rows.Close()

	for _, branchUndoLog := range undoLogs {
		sqlUndoLogs := branchUndoLog.SqlUndoLogs
		for _, sqlUndoLog := range sqlUndoLogs {
			tableMeta, err := GetTableMetaCache(conn.cfg.DBName).GetTableMeta(conn, sqlUndoLog.TableName)
			if err != nil {
				tx.Rollback()
				return errors.WithStack(err)
			}

			sqlUndoLog.SetTableMeta(tableMeta)
			err1 := NewMysqlUndoExecutor(*sqlUndoLog).Execute(conn)
			if err1 != nil {
				tx.Rollback()
				return errors.WithStack(err1)
			}
		}
	}

	if exists {
		_, err := conn.execAlways(DeleteUndoLogSql, args)
		if err != nil {
			tx.Rollback()
			return errors.WithStack(err)
		}
		fmt.Printf("xid %s branch %d, undo_log deleted with %s", xid, branchID,
			GlobalFinished.String())
		tx.Commit()
	} else {
		manager.insertUndoLogWithGlobalFinished(conn, xid, branchID,
			buildContext(GetUndoLogParser().GetName()), GetUndoLogParser().GetDefaultContent())
		tx.Commit()
	}
	return nil
}

func (manager MysqlUndoLogManager) DeleteUndoLog(conn *mysqlConn, xid string, branchID int64) error {
	args := []driver.Value{xid, branchID}
	result, err := conn.execAlways(DeleteUndoLogSql, args)
	if err != nil {
		return err
	}
	affectCount, _ := result.RowsAffected()
	fmt.Printf("%d undo log deleted by xid:%s and branchID:%d", affectCount, xid, branchID)
	return nil
}

func (manager MysqlUndoLogManager) BatchDeleteUndoLog(conn *mysqlConn, xids []string, branchIDs []int64) error {
	if xids == nil || branchIDs == nil || len(xids) == 0 || len(branchIDs) == 0 {
		return nil
	}
	xidSize := len(xids)
	branchIDSize := len(branchIDs)
	batchDeleteSql := toBatchDeleteUndoLogSql(xidSize, branchIDSize)
	var args = make([]driver.Value, 0, xidSize+branchIDSize)
	for _, xid := range xids {
		args = append(args, xid)
	}
	for _, branchID := range branchIDs {
		args = append(args, branchID)
	}
	result, err := conn.execAlways(batchDeleteSql, args)
	if err != nil {
		return err
	}
	affectCount, _ := result.RowsAffected()
	fmt.Printf("%d undo log deleted by xids:%v and branchIDs:%v", affectCount, xids, branchIDs)
	return nil
}

func (manager MysqlUndoLogManager) DeleteUndoLogByLogCreated(conn *mysqlConn, logCreated time.Time, limitRows int) (sql.Result, error) {
	args := []driver.Value{logCreated, limitRows}
	result, err := conn.execAlways(DeleteUndoLogByCreateSql, args)
	return result, err
}

func toBatchDeleteUndoLogSql(xidSize int, branchIDSize int) string {
	var sb strings.Builder
	fmt.Fprint(&sb, "DELETE FROM undo_log WHERE xid in ")
	fmt.Fprint(&sb, appendInParam(xidSize))
	fmt.Fprint(&sb, " AND branch_id in ")
	fmt.Fprint(&sb, appendInParam(branchIDSize))
	return sb.String()
}

func (manager MysqlUndoLogManager) insertUndoLogWithNormal(conn *mysqlConn, xid string, branchID int64,
	rollbackCtx string, undoLogContent []byte) error {
	return manager.insertUndoLog(conn, xid, branchID, rollbackCtx, undoLogContent, Normal)
}

func (manager MysqlUndoLogManager) insertUndoLogWithGlobalFinished(conn *mysqlConn, xid string, branchID int64,
	rollbackCtx string, undoLogContent []byte) error {
	return manager.insertUndoLog(conn, xid, branchID, rollbackCtx, undoLogContent, GlobalFinished)
}

func (manager MysqlUndoLogManager) insertUndoLog(conn *mysqlConn, xid string, branchID int64,
	rollbackCtx string, undoLogContent []byte, state State) error {
	args := []driver.Value{branchID, xid, rollbackCtx, undoLogContent, state}
	_, err := conn.execAlways(InsertUndoLogSql, args)
	return err
}

func buildContext(serializer string) string {
	return fmt.Sprintf("serializer=%s", serializer)
}

func getSerializer(context string) string {
	return context[10:]
}
