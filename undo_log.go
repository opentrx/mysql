package mysql

import (
	"github.com/opentrx/mysql/schema"
)

type sqlUndoLog struct {
	SqlType     SQLType
	TableName   string
	BeforeImage *schema.TableRecords
	AfterImage  *schema.TableRecords
}

func (undoLog *sqlUndoLog) SetTableMeta(tableMeta schema.TableMeta) {
	if undoLog.BeforeImage != nil {
		undoLog.BeforeImage.TableMeta = tableMeta
	}
	if undoLog.AfterImage != nil {
		undoLog.AfterImage.TableMeta = tableMeta
	}
}

func (undoLog *sqlUndoLog) GetUndoRows() *schema.TableRecords {
	if undoLog.SqlType == SQLType_UPDATE ||
		undoLog.SqlType == SQLType_DELETE {
		return undoLog.BeforeImage
	} else if undoLog.SqlType == SQLType_INSERT {
		return undoLog.AfterImage
	}
	return nil
}

type branchUndoLog struct {
	Xid         string
	BranchID    int64
	SqlUndoLogs []*sqlUndoLog
}
