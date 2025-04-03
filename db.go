package redisql

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	redis "github.com/go-redis/redis/v8"
)

// ColumnType represents supported data types for table columns
type ColumnType string

const (
	ColumnTypeInt    ColumnType = "int"
	ColumnTypeString ColumnType = "string"
	ColumnTypeFloat  ColumnType = "float"
	ColumnTypeBool   ColumnType = "bool"
	ColumnTypeJSON   ColumnType = "json"
)

// Redis key templates
const (
	TableSchemaKeyTemplate       = "schema:table:%s"      // Table metadata key
	TableRowIDCounterKeyTemplate = "counter:%s:rowid"     // Row ID counter key
	TableUniqueIndexKeyTemplate  = "idx:%s:unique:%s"     // Unique index key (table, column)
	TableNumericIndexKeyTemplate = "idx:%s:numeric:%s"    // Numeric index key (table, column)
	DataRowKeyTemplate           = "data:table:%s:row:%d" // Data row key (table, rowID)
	TableRowIDColumn             = "_rowid"               // Special column name for row IDs
)

// ColumnDefinition defines the structure of a table column
type ColumnDefinition struct {
	Name         string     `json:"name"`           // Column name
	Type         ColumnType `json:"type"`           // Data type
	NotNull      bool       `json:"not_null"`       // NOT NULL constraint
	DefaultValue any        `json:"default_value"`  // Default value
	IsPrimaryKey bool       `json:"is_primary_key"` // Whether column is primary key
}

// IndexDefinition defines the structure of a table index
type IndexDefinition struct {
	Column    string `json:"column"`     // Column name this index applies to
	IsUnique  bool   `json:"is_unique"`  // Whether index enforces uniqueness
	IsNumeric bool   `json:"is_numeric"` // Whether index supports range queries
}

// TableSchema defines the structure of a database table
type TableSchema struct {
	Name      string              `json:"name"`       // Table name
	Columns   []*ColumnDefinition `json:"columns"`    // List of columns
	Indexes   []*IndexDefinition  `json:"indexes"`    // List of indexes
	CreatedAt int64               `json:"created_at"` // Creation timestamp (unix)
}

// Column returns the column definition for the given column name
func (s *TableSchema) Column(col string) *ColumnDefinition {
	for _, c := range s.Columns {
		if c.Name == col {
			return c
		}
	}
	return nil
}

// HasColumn checks if the table contains a column with the given name
func (s *TableSchema) HasColumn(col string) bool {
	for _, c := range s.Columns {
		if c.Name == col {
			return true
		}
	}
	return false
}

// Database implements a SQL-like engine on top of Redis
type Database struct {
	client *redis.Client // Redis client
}

// NewDatabase creates a new Database instance
func NewDatabase(opts *redis.Options) *Database {
	return &Database{
		client: redis.NewClient(opts),
	}
}

// Close terminates the Redis connection
func (r *Database) Close() error {
	return r.client.Close()
}

// CreateTable creates a new table with the given schema
func (r *Database) CreateTable(ctx context.Context, table TableSchema) error {
	schemaKey := fmt.Sprintf(TableSchemaKeyTemplate, table.Name)
	exists, err := r.client.Exists(ctx, schemaKey).Result()
	if err != nil {
		return fmt.Errorf("failed to check table existence: %v", err)
	}
	if exists > 0 {
		return fmt.Errorf("table %s already exists", table.Name)
	}

	rowIDKey := fmt.Sprintf(TableRowIDCounterKeyTemplate, table.Name)
	if err := r.client.Set(ctx, rowIDKey, 0, 0).Err(); err != nil {
		return fmt.Errorf("failed to initialize rowid counter: %v", err)
	}

	table.CreatedAt = time.Now().Unix()
	table.Columns = append(table.Columns, &ColumnDefinition{
		Name:         TableRowIDColumn,
		Type:         ColumnTypeInt,
		IsPrimaryKey: true,
	})
	table.Indexes = append(table.Indexes, &IndexDefinition{
		Column:    TableRowIDColumn,
		IsUnique:  true,
		IsNumeric: true,
	})

	tableJSON, err := json.Marshal(table)
	if err != nil {
		return fmt.Errorf("failed to marshal table definition: %v", err)
	}
	if err := r.client.Set(ctx, schemaKey, tableJSON, 0).Err(); err != nil {
		return fmt.Errorf("failed to store table metadata: %v", err)
	}
	return nil
}

// GetTableSchema retrieves the schema definition for a table
func (r *Database) GetTableSchema(ctx context.Context, tableName string) (*TableSchema, error) {
	schemaKey := fmt.Sprintf(TableSchemaKeyTemplate, tableName)
	tableJSON, err := r.client.Get(ctx, schemaKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("table %s does not exist", tableName)
		}
		return nil, fmt.Errorf("failed to get table metadata: %v", err)
	}

	var tableDef TableSchema
	if err := json.Unmarshal([]byte(tableJSON), &tableDef); err != nil {
		return nil, fmt.Errorf("failed to unmarshal table definition: %v", err)
	}
	return &tableDef, nil
}

// DropTable removes a table and all its data
func (r *Database) DropTable(ctx context.Context, table string) error {
	tableDef, err := r.GetTableSchema(ctx, table)
	if err != nil {
		return err
	}

	var keysToDelete []string

	keysToDelete = append(keysToDelete,
		fmt.Sprintf(TableSchemaKeyTemplate, table),
		fmt.Sprintf(TableRowIDCounterKeyTemplate, table),
	)

	rowIDIndexKey := fmt.Sprintf(TableNumericIndexKeyTemplate, table, TableRowIDColumn)
	rowIDs, err := r.client.ZRange(ctx, rowIDIndexKey, 0, -1).Result()
	if err != nil {
		return fmt.Errorf("failed to fetch row IDs: %v", err)
	}

	for _, rowID := range rowIDs {
		rid, err := strconv.ParseInt(rowID, 10, 64)
		if err != nil {
			return err
		}
		rowKey := fmt.Sprintf(DataRowKeyTemplate, table, rid)
		keysToDelete = append(keysToDelete, rowKey)
	}

	for _, idx := range tableDef.Indexes {
		if idx.IsUnique {
			keysToDelete = append(keysToDelete,
				fmt.Sprintf(TableUniqueIndexKeyTemplate, table, idx.Column),
			)
		} else if idx.IsNumeric {
			keysToDelete = append(keysToDelete,
				fmt.Sprintf(TableNumericIndexKeyTemplate, table, idx.Column),
			)
		}
	}

	if err := r.client.Del(ctx, keysToDelete...).Err(); err != nil {
		return fmt.Errorf("failed to delete table data: %v", err)
	}

	return nil
}

// TableExists checks if a table exists
func (r *Database) TableExists(ctx context.Context, table string) (bool, error) {
	key := fmt.Sprintf(TableSchemaKeyTemplate, table)
	exists, err := r.client.Exists(ctx, key).Result()
	return exists > 0, err
}

// Insert adds a new row to the specified table
func (r *Database) Insert(ctx context.Context, table string, data map[string]interface{}) (int64, error) {
	schema, err := r.GetTableSchema(ctx, table)
	if err != nil {
		return 0, fmt.Errorf("table %s does not exist", table)
	}

	rowData := make(map[string]interface{})
	for _, col := range schema.Columns {
		colName := col.Name
		value, exists := data[colName]
		if !exists {
			if col.NotNull {
				if col.DefaultValue == nil {
					return 0, fmt.Errorf("column %s is NOT NULL but no value provided", colName)
				}
				value = col.DefaultValue
			} else {
				continue
			}
		}

		val := value
		switch col.Type {
		case ColumnTypeInt:
			switch v := value.(type) {
			case int:
				val = int64(v)
			case int16:
				val = int64(v)
			case int32:
				val = int64(v)
			case int64:
				val = v
			default:
				return 0, fmt.Errorf("column %s expects int type", colName)
			}
		case ColumnTypeBool:
			if _, ok := value.(bool); !ok {
				return 0, fmt.Errorf("column %s expects bool type", colName)
			}
		case ColumnTypeString:
			if _, ok := value.(string); !ok {
				return 0, fmt.Errorf("column %s expects string type", colName)
			}
			val = value
		case ColumnTypeFloat:
			switch v := value.(type) {
			case float32:
				val = float64(v)
			case float64:
				val = v
			default:
				return 0, fmt.Errorf("column %s expects float type", colName)
			}
		}
		rowData[colName] = val
	}

	for _, idx := range schema.Indexes {
		if !idx.IsUnique {
			continue
		}
		colValue, exists := rowData[idx.Column]
		if !exists {
			continue
		}

		idxKey := fmt.Sprintf(TableUniqueIndexKeyTemplate, table, idx.Column)

		// Check if member exists
		_, err = r.client.ZScore(ctx, idxKey, fmt.Sprintf("%v", colValue)).Result()
		if err == nil {
			return 0, fmt.Errorf("unique constraint violation on column %s (value: %v)", idx.Column, colValue)
		}
		if err != redis.Nil {
			return 0, fmt.Errorf("failed to check unique constraint: %v", err)
		}
	}

	rowIDKey := fmt.Sprintf(TableRowIDCounterKeyTemplate, table)
	rowID, err := r.client.Incr(ctx, rowIDKey).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to generate row ID: %v", err)
	}
	rowData[TableRowIDColumn] = rowID

	_, err = r.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		rowKey := fmt.Sprintf(DataRowKeyTemplate, table, rowID)
		var args []interface{}
		for col, value := range rowData {
			args = append(args, col, value)
		}
		pipe.HMSet(ctx, rowKey, args...)

		for _, idx := range schema.Indexes {
			colValue := rowData[idx.Column]
			if colValue == nil {
				continue
			}

			switch {
			case idx.IsUnique:
				idxKey := fmt.Sprintf(TableUniqueIndexKeyTemplate, table, idx.Column)
				pipe.ZAddNX(ctx, idxKey, &redis.Z{
					Score:  float64(rowID),
					Member: fmt.Sprintf("%v", colValue),
				})
			case idx.IsNumeric:
				idxKey := fmt.Sprintf(TableNumericIndexKeyTemplate, table, idx.Column)
				pipe.ZAdd(ctx, idxKey, &redis.Z{
					Score:  parseFloat(colValue),
					Member: rowID,
				})
			}
		}
		return nil
	})

	if err != nil {
		return 0, fmt.Errorf("failed to insert data: %v", err)
	}
	return rowID, nil
}

// Update modifies an existing row
func (r *Database) Update(ctx context.Context, table string, rowID int64, data map[string]interface{}) error {
	schema, err := r.GetTableSchema(ctx, table)
	if err != nil {
		return err
	}
	rowKey := fmt.Sprintf(DataRowKeyTemplate, table, rowID)
	oldData, err := r.client.HGetAll(ctx, rowKey).Result()
	if err != nil {
		return fmt.Errorf("row not found: %v", err)
	}

	_, err = r.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		var args []interface{}
		for col, value := range data {
			args = append(args, col, value)
		}
		pipe.HMSet(ctx, rowKey, args...)

		for _, idx := range schema.Indexes {
			oldValue := oldData[idx.Column]
			newValue := fmt.Sprintf("%v", data[idx.Column])

			if oldValue == newValue {
				continue
			}

			switch {
			case idx.IsUnique:
				idxKey := fmt.Sprintf(TableUniqueIndexKeyTemplate, table, idx.Column)
				exists, _ := r.client.ZScore(ctx, idxKey, newValue).Result()
				if exists > 0 {
					return fmt.Errorf("unique constraint violation on column %s", idx.Column)
				}
				pipe.ZRem(ctx, idxKey, oldValue)
				pipe.ZAddNX(ctx, idxKey, &redis.Z{
					Score:  float64(rowID),
					Member: newValue,
				})
			case idx.IsNumeric:
				idxKey := fmt.Sprintf(TableNumericIndexKeyTemplate, table, idx.Column)
				pipe.ZRem(ctx, idxKey, rowID)
				pipe.ZAdd(ctx, idxKey, &redis.Z{
					Score:  parseFloat(newValue),
					Member: rowID,
				})
			}
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to update: %v", err)
	}
	return nil
}

// Get retrieves a single row by its ID
func (r *Database) Get(ctx context.Context, table string, rowID int64) (map[string]interface{}, error) {
	exists, err := r.TableExists(ctx, table)
	if err != nil ||!exists {
		return nil, fmt.Errorf("table %s does not exist", table)
	}

	rowKey := fmt.Sprintf(DataRowKeyTemplate, table, rowID)
	data, err := r.client.HGetAll(ctx, rowKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("row %d not found", rowID)
		}
		return nil, fmt.Errorf("failed to get row data: %v", err)
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("row %d not found", rowID)
	}

	schema, err := r.GetTableSchema(ctx, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %v", err)
	}

	result := make(map[string]interface{})
	for _, col := range schema.Columns {
		raw, exists := data[col.Name]
		if !exists {
			continue
		}
		val, err := convertValueByType(raw, col.Type)
		if err != nil {
			return nil, fmt.Errorf("column %s type conversion failed: %v", col.Name, err)
		}
		result[col.Name] = val
	}

	return result, nil
}

// GetByUniqueIndex retrieves a row using a unique index
func (r *Database) GetByUniqueIndex(ctx context.Context, table, column string, value interface{}) (map[string]interface{}, error) {
	schema, err := r.GetTableSchema(ctx, table)
	if err != nil {
		return nil, fmt.Errorf("table %s does not exist", table)
	}

	var isUnique bool
	for _, idx := range schema.Indexes {
		if idx.Column == column && idx.IsUnique {
			isUnique = true
			break
		}
	}
	if !isUnique {
		return nil, fmt.Errorf("column %s is not a unique index", column)
	}

	indexKey := fmt.Sprintf(TableUniqueIndexKeyTemplate, table, column)
	strValue := fmt.Sprintf("%v", value)
	rowID, err := r.client.ZScore(ctx, indexKey, strValue).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("no row found with %s = %v", column, value)
		}
		return nil, fmt.Errorf("failed to query index: %v", err)
	}

	return r.Get(ctx, table, int64(rowID))
}

// Delete removes a row and all its indexes atomically
func (r *Database) Delete(ctx context.Context, table string, rowID int64) error {
	schema, err := r.GetTableSchema(ctx, table)
	if err != nil {
		return fmt.Errorf("table %s does not exist", table)
	}

	rowidIndexKey := fmt.Sprintf(TableUniqueIndexKeyTemplate, table,
