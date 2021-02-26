package timescaledb

import (
	"database/sql"
	"fmt"
	"github.com/timescale/tsbs/pkg/targets"
	"log"
	"regexp"
	"strings"

	_ "github.com/jackc/pgx/v4/stdlib"
)

const (
	tagsKey      = "tags"
	TimeValueIdx = "TIME-VALUE"
	ValueTimeIdx = "VALUE-TIME"
)

// allows for testing
var fatal = log.Fatalf

var tableCols = make(map[string][]string)

type dbCreator struct {
	driver  string
	ds      targets.DataSource
	connStr string
	connDB  string
	opts    *LoadingOptions
}

func (d *dbCreator) Init() {
	// read the headers before all else
	d.ds.Headers()
	d.initConnectString()
}

func (d *dbCreator) initConnectString() {
	// Needed to connect to user's database in order to drop/create db-name database
	re := regexp.MustCompile(`(dbname)=\S*\b`)
	d.connStr = strings.TrimSpace(re.ReplaceAllString(d.connStr, ""))

	if d.connDB != "" {
		d.connStr = fmt.Sprintf("dbname=%s %s", d.connDB, d.connStr)
	}
}

// MustConnect connects or exits on errors
func MustConnect(dbType, connStr string) *sql.DB {
	db, err := sql.Open(dbType, connStr)
	if err != nil {
		panic(err)
	}
	return db
}

func (d *dbCreator) DBExists(dbName string) bool {
	db := MustConnect(d.driver, d.connStr)
	defer db.Close()
	r := MustQuery(db, "SELECT 1 from pg_database WHERE datname = $1", dbName)
	defer r.Close()
	return r.Next()
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	db := MustConnect(d.driver, d.connStr)
	defer db.Close()
	MustExec(db, "DROP DATABASE IF EXISTS "+dbName)
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	db := MustConnect(d.driver, d.connStr)
	MustExec(db, "CREATE DATABASE "+dbName)
	db.Close()
	return nil
}

func (d *dbCreator) PostCreateDB(dbName string) error {
	dbBench := MustConnect(d.driver, d.opts.GetConnectString(dbName))
	defer dbBench.Close()

	headers := d.ds.Headers()
	tagNames := headers.TagKeys
	tagTypes := headers.TagTypes
	if d.opts.CreateMetricsTable {
		createTagsTable(dbBench, tagNames, tagTypes, d.opts.UseJSON)
	}
	// tableCols is a global map. Globally cache the available tags
	tableCols[tagsKey] = tagNames
	// tagTypes holds the type of each tag value (as strings from Go types (string, float32...))
	d.opts.TagColumnTypes = tagTypes

	// Create function to be used with partitions
	if d.opts.NativePartitions {
		d.createFunctionToGeneratePartitions(dbBench)
	}

	// Each table is defined in the dbCreator 'cols' list. The definition consists of a
	// comma separated list of the table name followed by its columns. Iterate over each
	// definition to update our global cache and create the requisite tables and indexes
	for tableName, columns := range headers.FieldKeys {
		// tableCols is a global map. Globally cache the available columns for the given table
		tableCols[tableName] = columns
		fieldDefs, indexDefs := d.getFieldAndIndexDefinitions(tableName, columns)
		if d.opts.CreateMetricsTable {
			d.createTableAndIndexes(dbBench, tableName, fieldDefs, indexDefs)
		}
	}
	return nil
}

// getFieldAndIndexDefinitions iterates over a list of table columns, populating lists of
// definitions for each desired field and index. Returns separate lists of fieldDefs and indexDefs
func (d *dbCreator) getFieldAndIndexDefinitions(tableName string, columns []string) ([]string, []string) {
	var fieldDefs []string
	var indexDefs []string
	var allCols []string

	partitioningField := tableCols[tagsKey][0]
	// If the user has specified that we should partition on the primary tags key, we
	// add that to the list of columns to create
	if d.opts.InTableTag {
		allCols = append(allCols, partitioningField)
	}

	allCols = append(allCols, columns...)
	extraCols := 0 // set to 1 when hostname is kept in-table
	for idx, field := range allCols {
		if len(field) == 0 {
			continue
		}
		fieldType := "DOUBLE PRECISION"
		idxType := d.opts.FieldIndex
		// This condition handles the case where we keep the primary tag key in the table
		// and partition on it. Since under the current implementation this tag is always
		// hostname, we set it to a TEXT field instead of DOUBLE PRECISION
		if d.opts.InTableTag && idx == 0 {
			fieldType = "TEXT"
			idxType = ""
			extraCols = 1
		}

		fieldDefs = append(fieldDefs, fmt.Sprintf("%s %s", field, fieldType))
		// If the user specifies indexes on additional fields, add them to
		// our index definitions until we've reached the desired number of indexes
		if d.opts.FieldIndexCount == -1 || idx < (d.opts.FieldIndexCount+extraCols) {
			indexDefs = append(indexDefs, d.getCreateIndexOnFieldCmds(tableName, field, idxType)...)
		}
	}
	return fieldDefs, indexDefs
}

func (d *dbCreator) createFunctionToGeneratePartitions(dbBench *sql.DB) {

    const createFuncDDL = `
		CREATE OR REPLACE FUNCTION gen_partitions( ptable     TEXT
                                                 , p_ts_start TEXT DEFAULT '2016-01-01T00:00:00Z'
                                                 , p_ts_end   TEXT DEFAULT '2016-01-04T00:00:00Z'
                                                 , p_interval INTERVAL  DEFAULT '2 hours'
                                               )
		RETURNS void
		LANGUAGE PLPGSQL
		AS
		$$
		DECLARE
                start_time         timestamp     := to_timestamp(p_ts_start, 'YYYY-MM-DDTHH24:MI:SSZ') ;
  				end_time           timestamp     := to_timestamp(p_ts_end,   'YYYY-MM-DDTHH24:MI:SSZ') ;
  				partition_size     interval      := p_interval;
		
  				next_range_start   timestamp     := start_time;
  				next_range_limit   timestamp     := start_time + partition_size;
  				create_table_text  text;
  				pcounter           integer       := 0;
		BEGIN
  			RAISE NOTICE 'Value[start_time]:% '        , start_time;
  			RAISE NOTICE 'Value[end_time]:% '          , end_time;
  			RAISE NOTICE 'Value[partition_size]:% '    , partition_size;
  			RAISE NOTICE 'Value[next_range_start]:% '  , next_range_start;
  			RAISE NOTICE 'Value[next_range_limit]:% '  , next_range_limit;

  			WHILE (next_range_start <= end_time)
  			LOOP
    			pcounter := pcounter +1;
    			RAISE NOTICE 'Value[next_range_start]:% '  , next_range_start;
    			RAISE NOTICE 'Value[next_range_limit]:% '  , next_range_limit;

    			create_table_text := 'CREATE TABLE '|| ptable ||'_p_'|| to_char(pcounter,'FM00000') || '_'||to_char(next_range_start,'YYYY_MM_DD_HH24_MI') || ' PARTITION OF '|| ptable ||' FOR VALUES FROM ('|| quote_literal(next_range_start) || ') TO ('|| quote_literal(next_range_limit) || ');';
    			EXECUTE create_table_text;

    			next_range_start := next_range_start + partition_size;  -- + '1 second'::interval;
    			next_range_limit := next_range_limit + partition_size;

    			RAISE NOTICE '------------------------------------------';
  			END LOOP;
  			pcounter := pcounter +1;
  			EXECUTE 'CREATE TABLE '|| ptable ||'_p_'|| pcounter ||'_default PARTITION OF '|| ptable|| ' DEFAULT;';

		END;
	$$; `
	MustExec(dbBench, createFuncDDL)

}


// createTableAndIndexes takes a list of field and index definitions for a given tableName and constructs
// the necessary table, index, and potential hypertable based on the user's settings
func (d *dbCreator) createTableAndIndexes(dbBench *sql.DB, tableName string, fieldDefs []string, indexDefs []string) {

    var partSuffix string

    if d.opts.NativePartitions {
        partSuffix = " PARTITION BY RANGE (time) "
    } else {
        partSuffix = " "
    }


	MustExec(dbBench, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	//	MustExec(dbBench, fmt.Sprintf("CREATE TABLE %s (time timestamptz, tags_id integer, %s, additional_tags JSONB DEFAULT NULL)", tableName, strings.Join(fieldDefs, ",")))
	MustExec(dbBench, fmt.Sprintf("CREATE TABLE %s (time timestamptz, tags_id integer, %s, additional_tags JSONB DEFAULT NULL) %s ", tableName, strings.Join(fieldDefs, ","), partSuffix))
	if d.opts.PartitionIndex {
		MustExec(dbBench, fmt.Sprintf("CREATE INDEX ON %s(tags_id, \"time\" DESC)", tableName))
	}

	// Only allow one or the other, it's probably never right to have both.
	// Experimentation suggests (so far) that for 100k devices it is better to
	// use --time-partition-index for reduced index lock contention.
	if d.opts.TimePartitionIndex {
		MustExec(dbBench, fmt.Sprintf("CREATE INDEX ON %s(\"time\" DESC, tags_id)", tableName))
	} else if d.opts.TimeIndex {
		MustExec(dbBench, fmt.Sprintf("CREATE INDEX ON %s(\"time\" DESC)", tableName))
	}

	for _, indexDef := range indexDefs {
		MustExec(dbBench, indexDef)
	}

    // TODO: Call Native parition code
    if d.opts.NativePartitions {
        MustExec(dbBench,fmt.Sprintf("SELECT gen_partitions('%s','%s','%s')", tableName, d.opts.NativePartitionsTimeStart, d.opts.NativePartitionsTimeEnd))
    }

	if d.opts.UseHypertable {
		MustExec(dbBench, "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
		if d.opts.NumberPartitions > 0 {
			MustExec(dbBench,
				fmt.Sprintf("SELECT create_hypertable('%s'::regclass, 'time'::name, partitioning_column => '%s'::name, number_partitions => %v::smallint, chunk_time_interval => %d, create_default_indexes=>FALSE)",
					tableName, "tags_id", d.opts.NumberPartitions, d.opts.ChunkTime.Nanoseconds()/1000))
		} else {
			MustExec(dbBench,
				fmt.Sprintf("SELECT create_hypertable('%s'::regclass, 'time'::name, chunk_time_interval => %d, create_default_indexes=>FALSE)",
					tableName, d.opts.ChunkTime.Nanoseconds()/1000))
		}
	}
}

func (d *dbCreator) getCreateIndexOnFieldCmds(hypertable, field, idxType string) []string {
	var ret []string
	for _, idx := range strings.Split(idxType, ",") {
		if idx == "" {
			continue
		}

		indexDef := ""
		if idx == TimeValueIdx {
			indexDef = fmt.Sprintf("(time DESC, %s)", field)
		} else if idx == ValueTimeIdx {
			indexDef = fmt.Sprintf("(%s, time DESC)", field)
		} else {
			fatal("Unknown index type %v", idx)
		}

		ret = append(ret, fmt.Sprintf("CREATE INDEX ON %s %s", hypertable, indexDef))
	}
	return ret
}

func createTagsTable(db *sql.DB, tagNames, tagTypes []string, useJSON bool) {
	MustExec(db, "DROP TABLE IF EXISTS tags")
	if useJSON {
		MustExec(db, "CREATE TABLE tags(id SERIAL PRIMARY KEY, tagset JSONB)")
		MustExec(db, "CREATE UNIQUE INDEX uniq1 ON tags(tagset)")
		MustExec(db, "CREATE INDEX idxginp ON tags USING gin (tagset jsonb_path_ops);")
		return
	}

	MustExec(db, generateTagsTableQuery(tagNames, tagTypes))
	MustExec(db, fmt.Sprintf("CREATE UNIQUE INDEX uniq1 ON tags(%s)", strings.Join(tagNames, ",")))
	MustExec(db, fmt.Sprintf("CREATE INDEX ON tags(%s)", tagNames[0]))
}

func generateTagsTableQuery(tagNames, tagTypes []string) string {
	tagColumnDefinitions := make([]string, len(tagNames))
	for i, tagName := range tagNames {
		pgType := serializedTypeToPgType(tagTypes[i])
		tagColumnDefinitions[i] = fmt.Sprintf("%s %s", tagName, pgType)
	}

	cols := strings.Join(tagColumnDefinitions, ", ")
	return fmt.Sprintf("CREATE TABLE tags(id SERIAL PRIMARY KEY, %s)", cols)
}

func extractTagNamesAndTypes(tags []string) ([]string, []string) {
	tagNames := make([]string, len(tags))
	tagTypes := make([]string, len(tags))
	for i, tagWithType := range tags {
		tagAndType := strings.Split(tagWithType, " ")
		if len(tagAndType) != 2 {
			panic("tag header has invalid format")
		}
		tagNames[i] = tagAndType[0]
		tagTypes[i] = tagAndType[1]
	}

	return tagNames, tagTypes
}

// MustExec executes query or exits on error
func MustExec(db *sql.DB, query string, args ...interface{}) sql.Result {
	r, err := db.Exec(query, args...)
	if err != nil {
		fmt.Printf("could not execute sql: %s", query)
		panic(err)
	}
	return r
}

// MustQuery executes query or exits on error
func MustQuery(db *sql.DB, query string, args ...interface{}) *sql.Rows {
	r, err := db.Query(query, args...)
	if err != nil {
		panic(err)
	}
	return r
}

// MustBegin starts transaction or exits on error
func MustBegin(db *sql.DB) *sql.Tx {
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}
	return tx
}

func serializedTypeToPgType(serializedType string) string {
	switch serializedType {
	case "string":
		return "TEXT"
	case "float32":
		return "FLOAT"
	case "float64":
		return "DOUBLE PRECISION"
	case "int64":
		return "BIGINT"
	case "int32":
		return "INTEGER"
	default:
		panic(fmt.Sprintf("unrecognized type %s", serializedType))
	}
}
