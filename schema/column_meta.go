package schema

type ColumnMeta struct {
	TableCat        string
	TableSchemeName string
	TableName       string
	ColumnName      string
	DataType        int32
	DataTypeName    string
	ColumnSize      int64
	DecimalDigits   int64
	NumPrecRadix    int64
	Nullable        int32
	Remarks         string
	ColumnDef       string
	SqlDataType     int32
	SqlDatetimeSub  int32
	CharOctetLength int64
	OrdinalPosition int64
	IsNullable      string
	IsAutoIncrement string
}
