package mysql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"time"
)

import (
	"github.com/google/go-cmp/cmp"
	"github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
)

import (
	"github.com/opentrx/mysql/schema"
)

var EXPIRE_TIME = 15 * time.Minute

var tableMetaCaches map[string]*TableMetaCache = make(map[string]*TableMetaCache, 0)

type TableMetaCache struct {
	tableMetaCache *cache.Cache
	dbName         string
}

func InitTableMetaCache(dbName string) {
	tableMetaCache := &TableMetaCache{
		tableMetaCache: cache.New(EXPIRE_TIME, 10*EXPIRE_TIME),
		dbName:         dbName,
	}
	tableMetaCaches[dbName] = tableMetaCache
}

func GetTableMetaCache(dbName string) *TableMetaCache {
	return tableMetaCaches[dbName]
}

func (cache *TableMetaCache) GetTableMeta(conn *mysqlConn, tableName string) (schema.TableMeta, error) {
	if tableName == "" {
		return schema.TableMeta{}, errors.New("TableMeta cannot be fetched without tableName")
	}
	cacheKey := cache.GetCacheKey(tableName)
	tMeta, found := cache.tableMetaCache.Get(cacheKey)
	if found {
		meta := tMeta.(schema.TableMeta)
		return meta, nil
	} else {
		meta, err := cache.FetchSchema(conn, tableName)
		if err != nil {
			return schema.TableMeta{}, errors.WithStack(err)
		}
		cache.tableMetaCache.Set(cacheKey, meta, EXPIRE_TIME)
		return meta, nil
	}
}

func (cache *TableMetaCache) Refresh(conn *mysqlConn, resourceID string) {
	for k, v := range cache.tableMetaCache.Items() {
		meta := v.Object.(schema.TableMeta)
		key := cache.GetCacheKey(meta.TableName)
		if k == key {
			tMeta, err := cache.FetchSchema(conn, meta.TableName)
			if err != nil {
				errLog.Print("get table meta error:%s", err.Error())
			}
			if !cmp.Equal(tMeta, meta) {
				cache.tableMetaCache.Set(key, tMeta, EXPIRE_TIME)
			}
		}
	}
}

func (cache *TableMetaCache) GetCacheKey(tableName string) string {
	return fmt.Sprintf("%s.%s", cache.dbName, escape(tableName, "`"))
}

func (cache *TableMetaCache) FetchSchema(conn *mysqlConn, tableName string) (schema.TableMeta, error) {
	tm := schema.TableMeta{TableName: tableName,
		AllColumns: make(map[string]schema.ColumnMeta),
		AllIndexes: make(map[string]schema.IndexMeta),
	}
	columnMetas, err := GetColumns(conn, cache.dbName, tableName)
	if err != nil {
		return schema.TableMeta{}, errors.Wrapf(err, "Could not found any index in the table: %s", tableName)
	}
	columns := make([]string, 0)
	for _, column := range columnMetas {
		tm.AllColumns[column.ColumnName] = column
		columns = append(columns, column.ColumnName)
	}
	tm.Columns = columns
	indexes, err := GetIndexes(conn, cache.dbName, tableName)
	if err != nil {
		return schema.TableMeta{}, errors.Wrapf(err, "Could not found any index in the table: %s", tableName)
	}
	for _, index := range indexes {
		col := tm.AllColumns[index.ColumnName]
		idx, ok := tm.AllIndexes[index.IndexName]
		if ok {
			idx.Values = append(idx.Values, col)
		} else {
			index.Values = append(index.Values, col)
			tm.AllIndexes[index.IndexName] = index
		}
	}
	if len(tm.AllIndexes) == 0 {
		return schema.TableMeta{}, errors.Errorf("Could not found any index in the table: %s", tableName)
	}

	return tm, nil
}

func GetColumns(conn *mysqlConn, dbName, tableName string) ([]schema.ColumnMeta, error) {
	var tn = escape(tableName, "`")

	args := []driver.Value{dbName, tn}
	//`TABLE_CATALOG`,	`TABLE_SCHEMA`,	`TABLE_NAME`,	`COLUMN_NAME`,	`ORDINAL_POSITION`,	`COLUMN_DEFAULT`,
	//`IS_NULLABLE`, `DATA_TYPE`,	`CHARACTER_MAXIMUM_LENGTH`,	`CHARACTER_OCTET_LENGTH`,	`NUMERIC_PRECISION`,
	//`NUMERIC_SCALE`, `DATETIME_PRECISION`, `CHARACTER_SET_NAME`,	`COLLATION_NAME`,	`COLUMN_TYPE`,	`COLUMN_KEY',
	//`EXTRA`,	`PRIVILEGES`, `COLUMN_COMMENT`, `GENERATION_EXPRESSION`, `SRS_ID`
	s := "SELECT `TABLE_CATALOG`, `TABLE_SCHEMA`, `TABLE_NAME`, `COLUMN_NAME`, `DATA_TYPE`, `CHARACTER_MAXIMUM_LENGTH`, " +
		"`NUMERIC_PRECISION`, `NUMERIC_SCALE`, `IS_NULLABLE`, `COLUMN_COMMENT`, `COLUMN_DEFAULT`, `CHARACTER_OCTET_LENGTH`, " +
		"`ORDINAL_POSITION`, `COLUMN_KEY`, `EXTRA`  FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?"

	rows, err := conn.prepareQuery(s, args)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]schema.ColumnMeta, 0)

	var tableCat, tScheme, tName, columnName, dataType, isNullable, remark, colDefault, colKey, extra sql.NullString
	var columnSize, decimalDigits, numPreRadix, charOctetLength, ordinalPosition sql.NullInt32

	vals := make([]driver.Value, 15)
	dest := []interface{}{
		&tableCat, &tScheme, &tName, &columnName, &dataType,
		&columnSize, &decimalDigits, &numPreRadix, &isNullable,
		&remark, &colDefault, &charOctetLength, &ordinalPosition,
		&colKey, &extra,
	}

	for {
		err := rows.Next(vals)
		if err != nil {
			break
		}

		for i, sv := range vals {
			err := convertAssignRows(dest[i], sv)
			if err != nil {
				return nil, fmt.Errorf(`sql: Scan error on column index %d, name %q: %v`, i, rows.Columns()[i], err)
			}
		}

		col := schema.ColumnMeta{}

		col.TableCat = tableCat.String
		col.TableSchemeName = tScheme.String
		col.TableName = tName.String
		col.ColumnName = strings.Trim(columnName.String, "` ")
		col.DataTypeName = dataType.String
		col.DataType = GetSqlDataType(dataType.String)
		col.ColumnSize = columnSize.Int32
		col.DecimalDigits = decimalDigits.Int32
		col.NumPrecRadix = numPreRadix.Int32
		col.IsNullable = isNullable.String
		if strings.ToLower(isNullable.String) == "yes" {
			col.Nullable = 1
		} else {
			col.Nullable = 0
		}
		col.Remarks = remark.String
		col.ColumnDef = colDefault.String
		col.SqlDataType = 0
		col.SqlDatetimeSub = 0
		col.CharOctetLength = charOctetLength.Int32
		col.OrdinalPosition = ordinalPosition.Int32
		col.IsAutoIncrement = extra.String

		result = append(result, col)
	}
	return result, nil
}

func GetIndexes(conn *mysqlConn, dbName, tableName string) ([]schema.IndexMeta, error) {
	var tn = escape(tableName, "`")
	args := []driver.Value{dbName, tn}

	//`TABLE_CATALOG`, `TABLE_SCHEMA`, `TABLE_NAME`, `NON_UNIQUE`, `INDEX_SCHEMA`, `INDEX_NAME`, `SEQ_IN_INDEX`,
	//`COLUMN_NAME`, `COLLATION`, `CARDINALITY`, `SUB_PART`, `PACKED`, `NULLABLE`, `INDEX_TYPE`, `COMMENT`,
	//`INDEX_COMMENT`, `IS_VISIBLE`, `EXPRESSION`
	s := "SELECT `INDEX_NAME`, `COLUMN_NAME`, `NON_UNIQUE`, `INDEX_TYPE`, `SEQ_IN_INDEX`, `COLLATION`, `CARDINALITY` " +
		"FROM `INFORMATION_SCHEMA`.`STATISTICS` WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?"

	rows, err := conn.prepareQuery(s, args)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]schema.IndexMeta, 0)

	var indexName, columnName, nonUnique, indexType, collation sql.NullString
	var ordinalPosition, cardinality sql.NullInt32

	vals := make([]driver.Value, 7)
	dest := []interface{}{
		&indexName, &columnName, &nonUnique, &indexType,
		&ordinalPosition, &collation, &cardinality,
	}

	for {
		err := rows.Next(vals)
		if err != nil {
			break
		}

		for i, sv := range vals {
			err := convertAssignRows(dest[i], sv)
			if err != nil {
				return nil, fmt.Errorf(`sql: Scan error on column index %d, name %q: %v`, i, rows.Columns()[i], err)
			}
		}

		index := schema.IndexMeta{
			Values: make([]schema.ColumnMeta, 0),
		}

		index.IndexName = indexName.String
		index.ColumnName = columnName.String
		if "yes" == strings.ToLower(nonUnique.String) || nonUnique.String == "1" {
			index.NonUnique = true
		}
		index.OrdinalPosition = ordinalPosition.Int32
		index.AscOrDesc = collation.String
		index.Cardinality = cardinality.Int32
		if "primary" == strings.ToLower(indexName.String) {
			index.IndexType = schema.IndexType_PRIMARY
		} else if !index.NonUnique {
			index.IndexType = schema.IndexType_UNIQUE
		} else {
			index.IndexType = schema.IndexType_NORMAL
		}

		result = append(result, index)
	}
	return result, nil
}

func escape(tableName, cutset string) string {
	var tn = tableName
	if strings.Contains(tableName, ".") {
		idx := strings.LastIndex(tableName, ".")
		tName := tableName[idx+1:]
		tn = strings.Trim(tName, cutset)
	} else {
		tn = strings.Trim(tableName, cutset)
	}
	return tn
}
