package mysql

import (
	"bytes"
	"fmt"
	"reflect"
	"time"
)

import (
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"vimagination.zapto.org/byteio"
)

import (
	"github.com/opentrx/mysql/schema"
)

type ProtoBufUndoLogParser struct {
}

func (parser ProtoBufUndoLogParser) GetName() string {
	return "protobuf"
}

func (parser ProtoBufUndoLogParser) GetDefaultContent() []byte {
	return []byte("[]")
}

func (parser ProtoBufUndoLogParser) Encode(branchUndoLog *branchUndoLog) []byte {
	pbBranchUndoLog := convertBranchSqlUndoLog(branchUndoLog)
	data, err := proto.Marshal(pbBranchUndoLog)
	if err != nil {
		panic(err)
	}
	return data
}

func (parser ProtoBufUndoLogParser) Decode(data []byte) *branchUndoLog {
	var pbBranchUndoLog = &PbBranchUndoLog{}
	err := proto.Unmarshal(data, pbBranchUndoLog)
	if err != nil {
		panic(err)
	}

	return convertPbBranchSqlUndoLog(pbBranchUndoLog)
}

func convertField(field *schema.Field) *PbField {
	pbField := &PbField{
		Name:    field.Name,
		KeyType: int32(field.KeyType),
		Type:    field.Type,
	}
	if field.Value == nil {
		return pbField
	}
	var buf bytes.Buffer
	w := byteio.BigEndianWriter{Writer: &buf}

	switch v := field.Value.(type) {
	case int64:
		w.WriteByte(byte(fieldTypeLongLong))
		w.WriteInt64(v)
		break
	case float32:
		w.WriteByte(byte(fieldTypeFloat))
		w.WriteFloat32(v)
		break
	case float64:
		w.WriteByte(byte(fieldTypeDouble))
		w.WriteFloat64(v)
		break
	case []uint8:
		w.WriteByte(byte(fieldTypeString))
		w.Write(v)
		break
	case time.Time:
		var a [64]byte
		var b = a[:0]

		if v.IsZero() {
			b = append(b, "0000-00-00"...)
		} else {
			loc, _ := time.LoadLocation("Local")
			b = v.In(loc).AppendFormat(b, timeFormat)
		}
		w.WriteByte(byte(fieldTypeTime))
		w.Write(b)
	default:
		panic(errors.Errorf("unsupport types:%s,%v", reflect.TypeOf(field.Value).String(), field.Value))
	}
	pbField.Value = buf.Bytes()
	return pbField
}

func convertPbField(pbField *PbField) *schema.Field {
	field := &schema.Field{
		Name:    pbField.Name,
		KeyType: schema.KeyType(pbField.KeyType),
		Type:    pbField.Type,
	}
	if pbField.Value == nil {
		return field
	}
	r := byteio.BigEndianReader{Reader: bytes.NewReader(pbField.Value)}
	valueType, _ := r.ReadByte()

	switch fieldType(valueType) {
	case fieldTypeLongLong:
		value, _, _ := r.ReadInt64()
		field.Value = value
	case fieldTypeFloat:
		value, _, _ := r.ReadFloat32()
		field.Value = value
	case fieldTypeDouble:
		value, _, _ := r.ReadFloat64()
		field.Value = value
	case fieldTypeString:
		field.Value = pbField.Value[1:]
	case fieldTypeTime:
		loc, _ := time.LoadLocation("Local")
		t, err := parseDateTime(
			pbField.Value[1:],
			loc,
		)
		if err != nil {
			panic(err)
		}
		field.Value = t
		break
	default:
		fmt.Printf("unsupport types:%v", valueType)
		break
	}
	return field
}

func convertRow(row *schema.Row) *PbRow {
	pbFields := make([]*PbField, 0)
	for _, field := range row.Fields {
		pbField := convertField(field)
		pbFields = append(pbFields, pbField)
	}
	pbRow := &PbRow{
		Fields: pbFields,
	}
	return pbRow
}

func convertPbRow(pbRow *PbRow) *schema.Row {
	fields := make([]*schema.Field, 0)
	for _, pbField := range pbRow.Fields {
		field := convertPbField(pbField)
		fields = append(fields, field)
	}
	row := &schema.Row{Fields: fields}
	return row
}

func convertTableRecords(records *schema.TableRecords) *PbTableRecords {
	pbRows := make([]*PbRow, 0)
	for _, row := range records.Rows {
		pbRow := convertRow(row)
		pbRows = append(pbRows, pbRow)
	}
	pbRecords := &PbTableRecords{
		TableName: records.TableName,
		Rows:      pbRows,
	}
	return pbRecords
}

func convertPbTableRecords(pbRecords *PbTableRecords) *schema.TableRecords {
	rows := make([]*schema.Row, 0)
	for _, pbRow := range pbRecords.Rows {
		row := convertPbRow(pbRow)
		rows = append(rows, row)
	}
	records := &schema.TableRecords{
		TableName: pbRecords.TableName,
		Rows:      rows,
	}
	return records
}

func convertSqlUndoLog(undoLog *sqlUndoLog) *PbSqlUndoLog {
	pbSqlUndoLog := &PbSqlUndoLog{
		SqlType:   int32(undoLog.SqlType),
		TableName: undoLog.TableName,
	}
	if undoLog.BeforeImage != nil {
		beforeImage := convertTableRecords(undoLog.BeforeImage)
		pbSqlUndoLog.BeforeImage = beforeImage
	}
	if undoLog.AfterImage != nil {
		afterImage := convertTableRecords(undoLog.AfterImage)
		pbSqlUndoLog.AfterImage = afterImage
	}

	return pbSqlUndoLog
}

func convertPbSqlUndoLog(pbSqlUndoLog *PbSqlUndoLog) *sqlUndoLog {
	sqlUndoLog := &sqlUndoLog{
		SqlType:   SQLType(pbSqlUndoLog.SqlType),
		TableName: pbSqlUndoLog.TableName,
	}
	if pbSqlUndoLog.BeforeImage != nil {
		beforeImage := convertPbTableRecords(pbSqlUndoLog.BeforeImage)
		sqlUndoLog.BeforeImage = beforeImage
	}
	if pbSqlUndoLog.AfterImage != nil {
		afterImage := convertPbTableRecords(pbSqlUndoLog.AfterImage)
		sqlUndoLog.AfterImage = afterImage
	}
	return sqlUndoLog
}

func convertBranchSqlUndoLog(branchUndoLog *branchUndoLog) *PbBranchUndoLog {
	sqlUndoLogs := make([]*PbSqlUndoLog, 0)
	for _, sqlUndoLog := range branchUndoLog.SqlUndoLogs {
		pbSqlUndoLog := convertSqlUndoLog(sqlUndoLog)
		sqlUndoLogs = append(sqlUndoLogs, pbSqlUndoLog)
	}
	pbBranchUndoLog := &PbBranchUndoLog{
		Xid:         branchUndoLog.Xid,
		BranchID:    branchUndoLog.BranchID,
		SqlUndoLogs: sqlUndoLogs,
	}
	return pbBranchUndoLog
}

func convertPbBranchSqlUndoLog(pbBranchUndoLog *PbBranchUndoLog) *branchUndoLog {
	sqlUndoLogs := make([]*sqlUndoLog, 0)
	for _, sqlUndoLog := range pbBranchUndoLog.SqlUndoLogs {
		sqlUndoLog := convertPbSqlUndoLog(sqlUndoLog)
		sqlUndoLogs = append(sqlUndoLogs, sqlUndoLog)
	}
	branchUndoLog := &branchUndoLog{
		Xid:         pbBranchUndoLog.Xid,
		BranchID:    pbBranchUndoLog.BranchID,
		SqlUndoLogs: sqlUndoLogs,
	}
	return branchUndoLog
}
