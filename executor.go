package mysql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"time"
)

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/transaction-wg/seata-golang/pkg/base/meta"
	"github.com/transaction-wg/seata-golang/pkg/util/mysql"
	sql2 "github.com/transaction-wg/seata-golang/pkg/util/sql"
)

import (
	"github.com/opentrx/mysql/schema"
)

type insertExecutor struct {
	mc          *mysqlConn
	originalSQL string
	stmt        *ast.InsertStmt
	args        []driver.Value
}

type deleteExecutor struct {
	mc          *mysqlConn
	originalSQL string
	stmt        *ast.DeleteStmt
	args        []driver.Value
}

type selectForUpdateExecutor struct {
	mc          *mysqlConn
	originalSQL string
	stmt        *ast.SelectStmt
	args        []driver.Value
}

type updateExecutor struct {
	mc          *mysqlConn
	originalSQL string
	stmt        *ast.UpdateStmt
	args        []driver.Value
}

func (executor *insertExecutor) GetTableName() string {
	var sb strings.Builder
	executor.stmt.Table.TableRefs.Left.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return sb.String()
}

func (executor *insertExecutor) GetInsertColumns() []string {
	result := make([]string, 0)
	for _, col := range executor.stmt.Columns {
		result = append(result, col.Name.String())
	}
	return result
}

func (executor *deleteExecutor) GetTableName() string {
	var sb strings.Builder
	executor.stmt.TableRefs.TableRefs.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return sb.String()
}

func (executor *deleteExecutor) GetWhereCondition() string {
	var sb strings.Builder
	executor.stmt.Where.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return sb.String()
}

func (executor *selectForUpdateExecutor) GetTableName() string {
	var sb strings.Builder
	table := executor.stmt.From.TableRefs.Left.(*ast.TableSource)
	table.Source.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return sb.String()
}

func (executor *selectForUpdateExecutor) GetWhereCondition() string {
	var sb strings.Builder
	executor.stmt.Where.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return sb.String()
}

func (executor *updateExecutor) GetTableName() string {
	var sb strings.Builder
	executor.stmt.TableRefs.TableRefs.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return sb.String()
}

func (executor *updateExecutor) GetWhereCondition() string {
	var sb strings.Builder
	executor.stmt.Where.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return sb.String()
}

func (executor *updateExecutor) GetUpdateColumns() []string {
	columns := make([]string, 0)

	for _, assignment := range executor.stmt.List {
		columns = append(columns, assignment.Column.Name.String())
	}
	return columns
}

func (executor *insertExecutor) Execute() (driver.Result, error) {
	beforeImage, err := executor.BeforeImage()
	if err != nil {
		return nil, err
	}
	result, err := executor.mc.execAlways(executor.originalSQL, executor.args)
	if err != nil {
		return result, err
	}

	afterImage, err := executor.AfterImage(result)

	if err != nil {
		return nil, err
	}
	executor.PrepareUndoLog(beforeImage, afterImage)
	return result, err
}

func (executor *insertExecutor) PrepareUndoLog(beforeImage, afterImage *schema.TableRecords) {
	if len(afterImage.Rows) == 0 {
		return
	}

	var lockKeyRecords = afterImage

	lockKeys := buildLockKey(lockKeyRecords)
	executor.mc.ctx.AppendLockKey(lockKeys)

	sqlUndoLog := buildUndoItem(SQLType_INSERT, executor.GetTableName(), beforeImage, afterImage)
	executor.mc.ctx.AppendUndoItem(sqlUndoLog)
}

func (executor *insertExecutor) BeforeImage() (*schema.TableRecords, error) {
	return nil, nil
}

func (executor *insertExecutor) AfterImage(result sql.Result) (*schema.TableRecords, error) {
	var afterImage *schema.TableRecords
	var err error
	pkValues := executor.getPKValuesByColumn()
	if executor.getPKIndex() >= 0 {
		afterImage, err = executor.BuildTableRecords(pkValues)
	} else {
		pk, _ := result.LastInsertId()
		afterImage, err = executor.BuildTableRecords([]driver.Value{pk})
	}
	if err != nil {
		return nil, err
	}
	return afterImage, nil
}

func (executor *insertExecutor) BuildTableRecords(pkValues []driver.Value) (*schema.TableRecords, error) {
	tableMeta, err := executor.getTableMeta()
	if err != nil {
		return nil, err
	}
	var sb strings.Builder
	fmt.Fprint(&sb, "SELECT ")
	var i = 0
	columnCount := len(tableMeta.Columns)
	for _, column := range tableMeta.Columns {
		fmt.Fprint(&sb, CheckAndReplace(column))
		i = i + 1
		if i < columnCount {
			fmt.Fprint(&sb, ",")
		} else {
			fmt.Fprint(&sb, " ")
		}
	}
	fmt.Fprintf(&sb, "FROM %s ", executor.GetTableName())
	fmt.Fprintf(&sb, " WHERE `%s` IN ", tableMeta.GetPKName())
	fmt.Fprint(&sb, appendInParam(len(pkValues)))

	rows, err := executor.mc.prepareQuery(sb.String(), pkValues)
	if err != nil {
		return nil, err
	}
	return buildRecords(tableMeta, rows), nil
}

func (executor *insertExecutor) getPKValuesByColumn() []driver.Value {
	pkValues := make([]driver.Value, 0)
	columnLen := executor.getColumnLen()
	pkIndex := executor.getPKIndex()
	for i, value := range executor.args {
		if i%columnLen == pkIndex {
			pkValues = append(pkValues, value)
		}
	}
	return pkValues
}

func (executor *insertExecutor) getPKIndex() int {
	insertColumns := executor.GetInsertColumns()
	tableMeta, _ := executor.getTableMeta()

	if insertColumns != nil && len(insertColumns) > 0 {
		for i, columnName := range insertColumns {
			if strings.EqualFold(tableMeta.GetPKName(), columnName) {
				return i
			}
		}
	} else {
		allColumns := tableMeta.Columns
		var idx = 0
		for _, column := range allColumns {
			if strings.EqualFold(tableMeta.GetPKName(), column) {
				return idx
			}
			idx = idx + 1
		}
	}
	return -1
}

func (executor *insertExecutor) getColumnLen() int {
	insertColumns := executor.GetInsertColumns()
	if insertColumns != nil {
		return len(insertColumns)
	}
	tableMeta, _ := executor.getTableMeta()

	return len(tableMeta.Columns)
}

func (executor *insertExecutor) getTableMeta() (schema.TableMeta, error) {
	tableMetaCache := GetTableMetaCache(executor.mc.cfg.DBName)
	return tableMetaCache.GetTableMeta(executor.mc, executor.GetTableName())
}

func (executor *deleteExecutor) Execute() (driver.Result, error) {
	beforeImage, err := executor.BeforeImage()
	if err != nil {
		return nil, err
	}
	result, err := executor.mc.execAlways(executor.originalSQL, executor.args)
	if err != nil {
		return result, err
	}
	afterImage, err := executor.AfterImage()
	if err != nil {
		return nil, err
	}
	executor.PrepareUndoLog(beforeImage, afterImage)
	return result, err
}

func (executor *deleteExecutor) PrepareUndoLog(beforeImage, afterImage *schema.TableRecords) {
	if len(beforeImage.Rows) == 0 {
		return
	}

	var lockKeyRecords = beforeImage

	lockKeys := buildLockKey(lockKeyRecords)
	executor.mc.ctx.AppendLockKey(lockKeys)

	sqlUndoLog := buildUndoItem(SQLType_DELETE, executor.GetTableName(), beforeImage, afterImage)
	executor.mc.ctx.AppendUndoItem(sqlUndoLog)
}

func (executor *deleteExecutor) BeforeImage() (*schema.TableRecords, error) {
	tableMeta, err := executor.getTableMeta()
	if err != nil {
		return nil, err
	}
	return executor.buildTableRecords(tableMeta)
}

func (executor *deleteExecutor) AfterImage() (*schema.TableRecords, error) {
	return nil, nil
}

func (executor *deleteExecutor) buildTableRecords(tableMeta schema.TableMeta) (*schema.TableRecords, error) {
	rows, err := executor.mc.prepareQuery(executor.buildBeforeImageSql(tableMeta), executor.args)
	if err != nil {
		return nil, err
	}
	return buildRecords(tableMeta, rows), nil
}

func (executor *deleteExecutor) buildBeforeImageSql(tableMeta schema.TableMeta) string {
	var b strings.Builder
	fmt.Fprint(&b, "SELECT ")
	var i = 0
	columnCount := len(tableMeta.Columns)
	for _, column := range tableMeta.Columns {
		fmt.Fprint(&b, mysql.CheckAndReplace(column))
		i = i + 1
		if i < columnCount {
			fmt.Fprint(&b, ",")
		} else {
			fmt.Fprint(&b, " ")
		}
	}
	fmt.Fprintf(&b, " FROM %s WHERE ", executor.GetTableName())
	fmt.Fprint(&b, executor.GetWhereCondition())
	fmt.Fprint(&b, " FOR UPDATE")
	return b.String()
}

func (executor *deleteExecutor) getTableMeta() (schema.TableMeta, error) {
	tableMetaCache := GetTableMetaCache(executor.mc.cfg.DBName)
	return tableMetaCache.GetTableMeta(executor.mc, executor.GetTableName())
}

func (executor *selectForUpdateExecutor) Execute(lockRetryInterval time.Duration, lockRetryTimes int) (driver.Rows, error) {
	tableMeta, err := executor.getTableMeta()
	if err != nil {
		return nil, err
	}
	rows, err := executor.mc.prepareQuery(executor.originalSQL, executor.args)
	if err != nil {
		return nil, err
	}
	selectPKRows := buildRecords(tableMeta, rows)
	lockKeys := buildLockKey(selectPKRows)
	if lockKeys == "" {
		return rows, err
	} else {
		if executor.mc.ctx.xid != "" {
			var lockable bool
			var err error
			for i := 0; i < lockRetryTimes; i++ {
				lockable, err = dataSourceManager.LockQuery(meta.BranchTypeAT,
					executor.mc.cfg.DBName, executor.mc.ctx.xid, lockKeys)
				if lockable && err == nil {
					break
				}
				time.Sleep(lockRetryInterval)
			}
			if err != nil {
				return nil, err
			}
		}
	}
	return rows, err
}

func (executor *selectForUpdateExecutor) getTableMeta() (schema.TableMeta, error) {
	tableMetaCache := GetTableMetaCache(executor.mc.cfg.DBName)
	return tableMetaCache.GetTableMeta(executor.mc, executor.GetTableName())
}

func (executor *updateExecutor) Execute() (driver.Result, error) {
	beforeImage, err := executor.BeforeImage()
	if err != nil {
		return nil, err
	}
	result, err := executor.mc.execAlways(executor.originalSQL, executor.args)
	if err != nil {
		return result, err
	}
	afterImage, err := executor.AfterImage(beforeImage)
	if err != nil {
		return nil, err
	}
	executor.PrepareUndoLog(beforeImage, afterImage)
	return result, err
}

func (executor *updateExecutor) PrepareUndoLog(beforeImage, afterImage *schema.TableRecords) {
	if len(beforeImage.Rows) == 0 &&
		(afterImage == nil || len(afterImage.Rows) == 0) {
		return
	}

	var lockKeyRecords = afterImage

	lockKeys := buildLockKey(lockKeyRecords)
	executor.mc.ctx.AppendLockKey(lockKeys)

	sqlUndoLog := buildUndoItem(SQLType_UPDATE, executor.GetTableName(), beforeImage, afterImage)
	executor.mc.ctx.AppendUndoItem(sqlUndoLog)
}

func (executor *updateExecutor) BeforeImage() (*schema.TableRecords, error) {
	tableMeta, err := executor.getTableMeta()
	if err != nil {
		return nil, err
	}
	return executor.buildTableRecords(tableMeta)
}

func (executor *updateExecutor) AfterImage(beforeImage *schema.TableRecords) (*schema.TableRecords, error) {
	if beforeImage.Rows == nil || len(beforeImage.Rows) == 0 {
		return nil, nil
	}

	tableMeta, err := executor.getTableMeta()
	if err != nil {
		return nil, err
	}
	afterImageSql := executor.buildAfterImageSql(tableMeta, beforeImage)
	var args = make([]driver.Value, 0)
	for _, field := range beforeImage.PKFields() {
		args = append(args, field.Value)
	}
	rows, err := executor.mc.prepareQuery(afterImageSql, args)
	if err != nil {
		return nil, err
	}
	return buildRecords(tableMeta, rows), nil
}

func (executor *updateExecutor) buildAfterImageSql(tableMeta schema.TableMeta, beforeImage *schema.TableRecords) string {
	var b strings.Builder
	fmt.Fprint(&b, "SELECT ")
	var i = 0
	columnCount := len(tableMeta.Columns)
	for _, columnName := range tableMeta.Columns {
		fmt.Fprint(&b, mysql.CheckAndReplace(columnName))
		i = i + 1
		if i < columnCount {
			fmt.Fprint(&b, ",")
		} else {
			fmt.Fprint(&b, " ")
		}
	}
	fmt.Fprintf(&b, " FROM %s ", executor.GetTableName())
	fmt.Fprintf(&b, "WHERE `%s` IN", tableMeta.GetPKName())
	fmt.Fprint(&b, sql2.AppendInParam(len(beforeImage.PKFields())))
	return b.String()
}

func (executor *updateExecutor) buildTableRecords(tableMeta schema.TableMeta) (*schema.TableRecords, error) {
	sql := executor.buildBeforeImageSql(tableMeta)
	argsCount := strings.Count(sql, "?")
	rows, err := executor.mc.prepareQuery(sql, executor.args[len(executor.args)-argsCount:])
	if err != nil {
		return nil, err
	}
	return buildRecords(tableMeta, rows), nil
}

func (executor *updateExecutor) buildBeforeImageSql(tableMeta schema.TableMeta) string {
	var b strings.Builder
	fmt.Fprint(&b, "SELECT ")
	var i = 0
	columnCount := len(tableMeta.Columns)
	for _, column := range tableMeta.Columns {
		fmt.Fprint(&b, mysql.CheckAndReplace(column))
		i = i + 1
		if i != columnCount {
			fmt.Fprint(&b, ",")
		} else {
			fmt.Fprint(&b, " ")
		}
	}
	fmt.Fprintf(&b, " FROM %s WHERE ", executor.GetTableName())
	fmt.Fprint(&b, executor.GetWhereCondition())
	fmt.Fprint(&b, " FOR UPDATE")
	return b.String()
}

func (executor *updateExecutor) getTableMeta() (schema.TableMeta, error) {
	tableMetaCache := GetTableMetaCache(executor.mc.cfg.DBName)
	return tableMetaCache.GetTableMeta(executor.mc, executor.GetTableName())
}

func appendInParam(size int) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "(")
	for i := 0; i < size; i++ {
		fmt.Fprintf(&sb, "?")
		if i < size-1 {
			fmt.Fprint(&sb, ",")
		}
	}
	fmt.Fprintf(&sb, ")")
	return sb.String()
}

func buildLockKey(lockKeyRecords *schema.TableRecords) string {
	if lockKeyRecords.Rows == nil || len(lockKeyRecords.Rows) == 0 {
		return ""
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, lockKeyRecords.TableName)
	fmt.Fprint(&sb, ":")
	fields := lockKeyRecords.PKFields()
	length := len(fields)
	for i, field := range fields {
		fmt.Fprint(&sb, field.Value)
		if i < length-1 {
			fmt.Fprint(&sb, ",")
		}
	}
	return sb.String()
}

func buildUndoItem(sqlType SQLType, tableName string, beforeImage, afterImage *schema.TableRecords) *sqlUndoLog {
	sqlUndoLog := &sqlUndoLog{
		SqlType:     sqlType,
		TableName:   tableName,
		BeforeImage: beforeImage,
		AfterImage:  afterImage,
	}
	return sqlUndoLog
}

func buildRecords(meta schema.TableMeta, rows driver.Rows) *schema.TableRecords {
	resultSet := rows.(*binaryRows)
	records := schema.NewTableRecords(meta)
	columns := resultSet.Columns()
	rs := make([]*schema.Row, 0)

	values := make([]driver.Value, len(columns))

	for {
		err := resultSet.Next(values)
		if err != nil {
			break
		}

		fields := make([]*schema.Field, 0, len(columns))
		for i, col := range columns {
			filed := &schema.Field{
				Name:  col,
				Type:  meta.AllColumns[col].DataType,
				Value: values[i],
			}
			if strings.ToLower(col) == strings.ToLower(meta.GetPKName()) {
				filed.KeyType = schema.PRIMARY_KEY
			}
			fields = append(fields, filed)
		}
		row := &schema.Row{Fields: fields}
		rs = append(rs, row)
	}
	records.Rows = rs
	return records
}
