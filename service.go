package main

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	//"strconv"
	"strings"
)

var (
	errUnknownTable    = errors.New("unknown table")
	errRecordNotFound  = errors.New("record not found")
	errNothingToUpdate = errors.New("nothing to update")
	errSomethingWrong  = errors.New("something wrong")
)

type DbStructure struct {
	db         *sql.DB
	tables     map[string]map[string]tableInfo
	tablesList []string
}

type tableInfo struct {
	Field   string
	Type    string
	Null    bool
	Default *string
	PK      bool
}

func NewDbExplorer(db *sql.DB) (*DbStructure, error) {
	res := &DbStructure{
		db:         db,
		tables:     make(map[string]map[string]tableInfo),
		tablesList: make([]string, 0),
	}
	if err := res.loadTables(); err != nil {
		return nil, err
	}

	return res, nil
}

func (db *DbStructure) loadTables() error {
	rows, err := db.db.Query("SHOW TABLES")
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return err
		}
		db.tables[table] = make(map[string]tableInfo)
		db.tablesList = append(db.tablesList, table)

	}
	rows.Close()
	for table := range db.tables {
		if err := db.loadTableInfo(table); err != nil {
			return err
		}
	}

	return nil
}
func (db *DbStructure) loadTableInfo(table string) error {
	rows, err := db.db.Query(`SHOW FULL COLUMNS FROM ` + table)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	fields, ok := db.tables[table]
	if !ok {
		return errors.New("some error")
	}

	for rows.Next() {
		var collation *string
		var null, key, extra, privileges, comment string
		info := tableInfo{}
		if err := rows.Scan(&info.Field, &info.Type, &collation, &null, &key, &info.Default, &extra, &privileges, &comment); err != nil {
			return err
		}
		info.Null = null == "YES"
		info.PK = key == "PRI" && extra == "auto_increment"

		fields[info.Field] = info
	}
	return nil
}

func (db *DbStructure) getTablesList() ([]string, error) {
	return db.tablesList, nil
}

func (db *DbStructure) tableExists(table string) error {
	if _, ok := db.tables[table]; !ok {
		return errUnknownTable
	}
	return nil
}

func (db *DbStructure) getTablePK(table string) (string, error) {
	for field := range db.tables[table] {
		if db.tables[table][field].PK {
			return field, nil
		}
	}
	//todo error
	return "", nil
}

func (db *DbStructure) getTableData(table string, offset, limit int) ([]interface{}, error) {
	var query strings.Builder
	query.WriteString("SELECT * FROM ")
	query.WriteString(table)
	query.WriteString(" LIMIT ? OFFSET ?")

	rows, err := db.db.Query(query.String(), limit, offset)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	finalRows := make([]interface{}, 0)
	for rows.Next() {
		rowItems, err := scanRows(rows)
		if err != nil {
			panic(err)
		}
		finalRows = append(finalRows, rowItems)
	}
	return finalRows, nil
}
func (db *DbStructure) getTableItem(table string, id int) (interface{}, error) {
	tablePK, err := db.getTablePK(table)
	if err != nil {
		return "", err
	}

	var query strings.Builder
	query.WriteString("SELECT * FROM ")
	query.WriteString(table)
	query.WriteString(" WHERE ")
	query.WriteString(tablePK)
	query.WriteString("  = ?")

	rows, err := db.db.Query(query.String(), id)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var rowItems interface{}
	for rows.Next() {
		rowItems, err = scanRows(rows)
		if err != nil {
			return nil, err
		}
		return rowItems, nil
	}
	return nil, errRecordNotFound
}

func scanRows(rows *sql.Rows) (interface{}, error) {

	columns, err := rows.Columns()
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		panic(err)
	}

	colNum := len(columns)
	rawRowItems := make([]interface{}, len(columns))

	for i := 0; i < colNum; i++ {
		rawRowItems[i] = new(interface{})
	}
	if err := rows.Scan(rawRowItems...); err != nil {
		panic(err)
	}
	rowItems := make(map[string]interface{}, colNum)
	for i, col := range columns {
		rowItems[col] = *rawRowItems[i].(*interface{})
	}
	//todo fix
	types := make(map[string]string)
	for _, col := range columnTypes {
		types[col.Name()] = col.DatabaseTypeName()
	}

	//todo fix types
	for field, val := range rowItems {
		switch val.(type) {
		case int:
			if v, ok := val.(int); ok {
				rowItems[field] = v
			}
		case int64:
			if v, ok := val.(int64); ok {
				rowItems[field] = v
			}
		case float64:
			if v, ok := val.(float64); ok {
				rowItems[field] = v
			}
		case []byte:
			if v, ok := val.([]byte); ok {
				res := string(v)
				if types[field] == "INT" {
					intRes, err := strconv.Atoi(res)
					if err != nil {
						panic(err)
					}
					rowItems[field] = intRes
				} else {
					rowItems[field] = res
				}
			}
		case string:
			if v, ok := val.(string); ok {
				rowItems[field] = v
			}
		case nil:
			rowItems[field] = nil
		default:
			fmt.Println("unknown type")
		}
	}
	return rowItems, nil
}
func (db *DbStructure) deleteTableItem(table string, id int) (int64, error) {
	var query strings.Builder
	query.WriteString("DELETE FROM ")
	query.WriteString(table)
	query.WriteString(" WHERE ID = ?")
	result, err := db.db.Exec(query.String(), id)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (db *DbStructure) createItem(table string, a interface{}) (int64, error) {
	m, ok := a.(map[string]interface{})
	if !ok {
		return 0, errors.New("err")
	}
	//todo check field types
	//todo rename tableInfo or/and tableI
	pk, err := db.getTablePK(table)
	if err != nil {
		return 0, err
	}
	tableI := db.tables[table]
	for col := range m {
		if col == pk {
			continue
		}
		fieldInfo, ok := tableI[col]
		if !ok {
			continue
		}
		switch m[col].(type) {
		case int:
			if !strings.Contains(fieldInfo.Type, "int") {
				//todo вынести error
				return 0, errors.New("field " + col + " have invalid type")
			}
		case string:
			if !strings.Contains(fieldInfo.Type, "varchar") && !strings.Contains(fieldInfo.Type, "text") {
				return 0, errors.New("field " + col + " have invalid type")
			}
		case float32, float64:
			if !strings.Contains(fieldInfo.Type, "float") && !strings.Contains(fieldInfo.Type, "double") {
				return 0, errors.New("field " + col + " have invalid type")
			}
		case nil:
			if !fieldInfo.Null {
				return 0, errors.New("field " + col + " have invalid type")
			}
		default:
			return 0, errors.New("invalid field type")
		}
	}
	columns := make([]string, 0, len(tableI))
	values := make([]interface{}, 0, len(tableI))
	for field, value := range tableI {
		if value.PK != true {
			if v, ok := m[field]; ok {
				columns = append(columns, field)
				values = append(values, v)
			}
		}
	}
	var defCols []string
	var defValues []interface{}
	fields := db.tables[table]

	for k, field := range fields {
		if field.PK || field.Null {
			continue
		}
		if field.Default == nil {
			if _, ok := m[k]; !ok {
				defCols = append(defCols, k)
				if strings.Contains(field.Type, "int") {
					var v int
					defValues = append(defValues, v)
				}
				if strings.Contains(field.Type, "varchar") || strings.Contains(field.Type, "text") {
					var v string
					defValues = append(defValues, v)
				}
				if strings.Contains(field.Type, "float") || strings.Contains(field.Type, "double") {
					var v float64
					defValues = append(defValues, v)
				}
			}
		}
	}

	if defCols != nil && defValues != nil {
		columns = append(columns, defCols...)
		values = append(values, defValues...)
	}

	var params string
	if len(values) == 1 {
		params = "?"
	} else {
		params = "?"
		params += strings.Repeat(", ?", len(values)-2)
		params += ", ?"
	}

	var query strings.Builder
	query.WriteString("INSERT INTO ")
	query.WriteString(table)
	query.WriteString(" (")
	query.WriteString(strings.Join(columns, ", "))
	query.WriteString(")")
	query.WriteString(" VALUES(")
	query.WriteString(params)
	query.WriteString(")")

	result, err := db.db.Exec(query.String(), values...)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
	return 0, nil
}
func (db *DbStructure) updateItem(table string, id int, a interface{}) (int64, error) {
	m, ok := a.(map[string]interface{})
	if !ok {
		return 0, errors.New("update failed")
	}
	pk, err := db.getTablePK(table)
	if err != nil {
		return 0, err
	}
	tableI := db.tables[table]
	for col := range m {
		if col == pk {
			return 0, errors.New("field " + col + " have invalid type")
		}
		fieldInfo, ok := tableI[col]
		if !ok {
			continue
		}
		switch m[col].(type) {
		case int:
			if !strings.Contains(fieldInfo.Type, "int") {
				//todo вынести error
				return 0, errors.New("field " + col + " have invalid type")
			}
		case string:
			if !strings.Contains(fieldInfo.Type, "varchar") && !strings.Contains(fieldInfo.Type, "text") {
				return 0, errors.New("field " + col + " have invalid type")
			}
		case float32, float64:
			if !strings.Contains(fieldInfo.Type, "float") && !strings.Contains(fieldInfo.Type, "double") {
				return 0, errors.New("field " + col + " have invalid type")
			}
		case nil:
			if !fieldInfo.Null {
				return 0, errors.New("field " + col + " have invalid type")
			}
		default:
			return 0, errors.New("invalid field type")
		}
	}

	columns := make([]string, 0, len(tableI))
	values := make([]interface{}, 0, len(tableI))
	for field, value := range tableI {
		if value.PK != true {
			if v, ok := m[field]; ok {
				columns = append(columns, field)
				values = append(values, v)
			}
		}
	}
	if len(columns) == 0 {
		return 0, errors.New("nothing to update")
	}

	//

	//
	cols := strings.Join(columns, " = ?, ")
	cols += " = ? "
	values = append(values, id)

	var query strings.Builder
	query.WriteString("UPDATE ")
	query.WriteString(table)
	query.WriteString(" SET ")
	query.WriteString(cols)
	query.WriteString(" WHERE ")
	query.WriteString(pk)
	query.WriteString(" = ?")

	result, err := db.db.Exec(query.String(), values...)
	if err != nil {
		return 0, err
	}
	rowsAffected, err := result.RowsAffected()
	if rowsAffected == 0 {
		return 0, errNothingToUpdate
	}
	return rowsAffected, err
}
