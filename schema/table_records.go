package schema

import (
	"strings"
)

type TableRecords struct {
	TableMeta TableMeta `json:"-"`
	TableName string
	Rows      []*Row
}

func NewTableRecords(meta TableMeta) *TableRecords {
	return &TableRecords{
		TableMeta: meta,
		TableName: meta.TableName,
		Rows:      make([]*Row, 0),
	}
}

func (records *TableRecords) PKFields() []*Field {
	pkRows := make([]*Field, 0)
	pk := records.TableMeta.GetPKName()
	for _, row := range records.Rows {
		for _, field := range row.Fields {
			if strings.ToLower(field.Name) == strings.ToLower(pk) {
				pkRows = append(pkRows, field)
				break
			}
		}
	}
	return pkRows
}
