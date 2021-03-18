package mysql

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
)

import (
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/transaction-wg/seata-golang/pkg/util/log"
)

import (
	"github.com/opentrx/mysql/schema"
)

const (
	InsertSqlTemplate = "INSERT INTO %s (%s) VALUES (%s)"
	DeleteSqlTemplate = "DELETE FROM %s WHERE `%s` = ?"
	UpdateSqlTemplate = "UPDATE %s SET %s WHERE `%s` = ?"
	SelectSqlTemplate = "SELECT %s FROM %s WHERE `%s` IN %s"
)

type BuildUndoSql func(undoLog sqlUndoLog) string

func DeleteBuildUndoSql(undoLog sqlUndoLog) string {
	beforeImage := undoLog.BeforeImage
	beforeImageRows := beforeImage.Rows

	if beforeImageRows == nil || len(beforeImageRows) == 0 {
		return ""
	}

	row := beforeImageRows[0]
	fields := row.NonPrimaryKeys()
	pkField := row.PrimaryKeys()[0]
	// PK is at last one.
	fields = append(fields, pkField)

	var sbCols, sbVals strings.Builder
	var size = len(fields)
	for i, field := range fields {
		fmt.Fprintf(&sbCols, "`%s`", field.Name)
		fmt.Fprint(&sbVals, "?")
		if i < size-1 {
			fmt.Fprint(&sbCols, ", ")
			fmt.Fprint(&sbVals, ", ")
		}
	}
	insertColumns := sbCols.String()
	insertValues := sbVals.String()

	return fmt.Sprintf(InsertSqlTemplate, undoLog.TableName, insertColumns, insertValues)
}

func InsertBuildUndoSql(undoLog sqlUndoLog) string {
	afterImage := undoLog.AfterImage
	afterImageRows := afterImage.Rows
	if afterImageRows == nil || len(afterImageRows) == 0 {
		return ""
	}
	row := afterImageRows[0]
	pkField := row.PrimaryKeys()[0]
	return fmt.Sprintf(DeleteSqlTemplate, undoLog.TableName, pkField.Name)
}

func UpdateBuildUndoSql(undoLog sqlUndoLog) string {
	beforeImage := undoLog.BeforeImage
	beforeImageRows := beforeImage.Rows

	if beforeImageRows == nil || len(beforeImageRows) == 0 {
		return ""
	}

	row := beforeImageRows[0]
	nonPkFields := row.NonPrimaryKeys()
	pkField := row.PrimaryKeys()[0]

	var sb strings.Builder
	var size = len(nonPkFields)
	for i, field := range nonPkFields {
		fmt.Fprintf(&sb, "`%s` = ?", field.Name)
		if i < size-1 {
			fmt.Fprint(&sb, ", ")
		}
	}
	updateColumns := sb.String()

	return fmt.Sprintf(UpdateSqlTemplate, undoLog.TableName, updateColumns, pkField.Name)
}

type MysqlUndoExecutor struct {
	sqlUndoLog sqlUndoLog
}

func NewMysqlUndoExecutor(undoLog sqlUndoLog) MysqlUndoExecutor {
	return MysqlUndoExecutor{sqlUndoLog: undoLog}
}

func (executor MysqlUndoExecutor) Execute(conn *mysqlConn) error {
	goOn, err := executor.dataValidationAndGoOn(conn)
	if err != nil {
		return err
	}
	if !goOn {
		return nil
	}

	var undoSql string
	var undoRows schema.TableRecords
	switch executor.sqlUndoLog.SqlType {
	case SQLType_INSERT:
		undoSql = InsertBuildUndoSql(executor.sqlUndoLog)
		undoRows = *executor.sqlUndoLog.AfterImage
		break
	case SQLType_DELETE:
		undoSql = DeleteBuildUndoSql(executor.sqlUndoLog)
		undoRows = *executor.sqlUndoLog.BeforeImage
		break
	case SQLType_UPDATE:
		undoSql = UpdateBuildUndoSql(executor.sqlUndoLog)
		undoRows = *executor.sqlUndoLog.BeforeImage
		break
	default:
		panic(errors.Errorf("unsupport sql type:%s", executor.sqlUndoLog.SqlType.String()))
	}

	if undoSql == "" {
		return nil
	}

	// PK is at last one.
	// INSERT INTO a (x, y, z, pk) VALUES (?, ?, ?, ?)
	// UPDATE a SET x=?, y=?, z=? WHERE pk = ?
	// DELETE FROM a WHERE pk = ?

	for _, row := range undoRows.Rows {
		var args = make([]driver.Value, 0)
		var pkValue interface{}

		for _, field := range row.Fields {
			if field.KeyType == schema.PRIMARY_KEY {
				pkValue = field.Value
			} else {
				if executor.sqlUndoLog.SqlType != SQLType_INSERT {
					args = append(args, field.Value)
				}
			}
		}
		args = append(args, pkValue)
		_, err = conn.execAlways(undoSql, args)
		if err != nil {
			return err
		}
	}
	return nil
}

func (executor MysqlUndoExecutor) dataValidationAndGoOn(conn *mysqlConn) (bool, error) {
	if executor.sqlUndoLog.BeforeImage != nil && executor.sqlUndoLog.AfterImage == nil {
		return true, nil
	}
	beforeEqualsAfterResult := cmp.Equal(executor.sqlUndoLog.BeforeImage, executor.sqlUndoLog.AfterImage)
	if beforeEqualsAfterResult {
		log.Info("Stop rollback because there is no data change between the before data snapshot and the after data snapshot.")
		return false, nil
	}
	currentRecords, err := executor.queryCurrentRecords(conn)
	if err != nil {
		return false, err
	}
	afterEqualsCurrentResult := cmp.Equal(executor.sqlUndoLog.AfterImage, currentRecords)
	if !afterEqualsCurrentResult {
		// If current data is not equivalent to the after data, then compare the current data with the before
		// data, too. No need continue to undo if current data is equivalent to the before data snapshot
		beforeEqualsCurrentResult := cmp.Equal(executor.sqlUndoLog.BeforeImage, currentRecords)
		if beforeEqualsCurrentResult {
			log.Info("Stop rollback because there is no data change between the before data snapshot and the after data snapshot.")
			return false, nil
		} else {
			oldRows, _ := json.Marshal(executor.sqlUndoLog.AfterImage.Rows)
			newRows, _ := json.Marshal(currentRecords.Rows)
			log.Errorf("check dirty datas failed, old and new data are not equal, tableName:[%s], oldRows:[%s], newRows:[%s].",
				executor.sqlUndoLog.TableName, string(oldRows), string(newRows))
			return false, errors.New("Has dirty records when undo.")
		}
	}
	return true, nil
}

func (executor MysqlUndoExecutor) queryCurrentRecords(conn *mysqlConn) (*schema.TableRecords, error) {
	undoRecords := executor.sqlUndoLog.GetUndoRows()
	tableMeta := undoRecords.TableMeta
	pkName := tableMeta.GetPKName()

	pkFields := undoRecords.PKFields()
	if pkFields == nil || len(pkFields) == 0 {
		return nil, nil
	}

	var pkValues = make([]driver.Value, 0)
	for _, field := range pkFields {
		pkValues = append(pkValues, field.Value)
	}

	var b strings.Builder
	var i = 0
	columnCount := len(tableMeta.Columns)
	for _, columnName := range tableMeta.Columns {
		fmt.Fprint(&b, CheckAndReplace(columnName))
		i = i + 1
		if i < columnCount {
			fmt.Fprint(&b, ",")
		} else {
			fmt.Fprint(&b, " ")
		}
	}

	inCondition := appendInParam(len(pkValues))
	selectSql := fmt.Sprintf(SelectSqlTemplate, b.String(), tableMeta.TableName, pkName, inCondition)
	rows, err := conn.prepareQuery(selectSql, pkValues)
	if err != nil {
		return nil, err
	}
	return buildRecords(tableMeta, rows), nil
}
