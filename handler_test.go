package presto

import (
	"encoding/json"
	"testing"

	"golang.org/x/net/context"
)

var h *Handler
var defaultSchema *string

func init() {
	h = NewHandler(".testdata")
	s := "myexcels"
	defaultSchema = &s
}

func TestListSchemaNames(t *testing.T) {
	sn, err := h.PrestoListSchemaNames(context.Background())
	if err != nil {
		t.Error("Error list schema names ", err)
	}

	if len(sn) != 1 && sn[0] != "myexcels" {
		t.Errorf("Expected %v got %+v", defaultSchema, sn[0])
	}
}

func TestListTableNames(t *testing.T) {

	sn := NewPrestoThriftNullableSchemaName()
	sn.SchemaName = defaultSchema

	tns, err := h.PrestoListTables(context.Background(), sn)
	if err != nil {
		t.Error("Error list schema names ", err)
	}

	expected := []string{"taba", "tabb", "tabc"}
	if len(expected) != len(tns) {
		t.Errorf("expected %v table names, got %v", len(expected), len(tns))
	}

	for idx, tn := range tns {
		if tn.TableName != expected[idx] {
			t.Errorf("Expected %v got %+v", expected[idx], tn)
		}
	}
}

func TestTableMetadata(t *testing.T) {

	tm := NewPrestoThriftSchemaTableName()
	tm.SchemaName = *defaultSchema
	tm.TableName = "taba"

	tmd, err := h.PrestoGetTableMetadata(context.Background(), tm)
	if err != nil {
		t.Error("Error get table metadata ", err)
	}

	if tmd == nil {
		t.Errorf("Table metadata is nil %+v", tmd)
		t.Fail()
	}

	if len(tmd.TableMetadata.Columns) == 0 {
		t.Errorf("Table metadata is nil or has zero colums %+v", tmd)
	}

	expectedNames := []string{"integer_column", "float_column", "date_column", "char_column", "bool_column"}
	expectedColTyps := []string{"DOUBLE", "DOUBLE", "TIMESTAMP", "VARCHAR", "DOUBLE"}
	for idx, tm := range tmd.TableMetadata.Columns {
		if expectedNames[idx] != tm.Name {
			t.Errorf("Wrong column, expected %v got %v", expectedNames[idx], tm.Name)
		}
		if expectedColTyps[idx] != tm.Type {
			t.Errorf("Wrong column type, expected %v got %v", expectedColTyps[idx], tm.Type)
		}
	}
}

func TestSplits(t *testing.T) {

	tm := NewPrestoThriftSchemaTableName()
	tm.SchemaName = *defaultSchema
	tm.TableName = "taba"

	splts, err := h.PrestoGetSplits(context.Background(), tm, nil, nil, 10, nil)
	if err != nil {
		t.Error("Error getting table splits ", err)
	}

	if splts == nil {
		t.Errorf("Table splits are nil %+v", splts)
	}

	expected := []string{"testfile.xlsx", "testfile_2.xlsx", "testfile_3.xlsx"}

	var bbs BlobStoreSplit
	for idx, sp := range splts.Splits {
		err := json.Unmarshal(sp.SplitId.ID, &bbs)
		if err != nil {
			t.Errorf("Splits %+v", sp)
		}
		if expected[idx] != bbs.File {
			t.Errorf("Expected %v got %v", expected[idx], bbs.File)
		}
	}
}

func TestPrestoGetRows(t *testing.T) {

	bsp := BlobStoreSplit{Schema: "myexcels", Table: "taba", File: "testfile.xlsx", Sheet: "Sheet1"}
	byts, err := json.Marshal(bsp)
	if err != nil {
		t.Error("Unable to serialize request", err)
	}

	xlsCols := []string{"integer_column", "float_column", "date_column", "char_column", "bool_column"}
	prestoTyps := []string{"DOUBLE", "DOUBLE", "TIMESTAMP", "VARCHAR", "DOUBLE"}

	pti := PrestoThriftId{ID: byts}
	rowz, err := h.PrestoGetRows(context.Background(), &pti, xlsCols, 16*1024*1024, nil)
	if err != nil {
		t.Error("Unable to perform request", err)
	}

	if rowz.RowCount != 6218 {
		t.Errorf("Row count error, expected %v got %v rows", 6218, rowz.RowCount)
	}

	if len(rowz.ColumnBlocks) != len(xlsCols) {
		t.Errorf("Column count error, expected %v got %v", len(xlsCols), len(rowz.ColumnBlocks))
	}

	for idx, cb := range rowz.ColumnBlocks {
		switch prestoTyps[idx] {
		case "DOUBLE":
			if int(rowz.RowCount) != len(cb.DoubleData.Doubles) {
				t.Errorf("double array sizes do not match, expected %v got %v", rowz.RowCount, len(cb.DoubleData.Doubles))
			}
		case "VARCHAR":
			if int(rowz.RowCount) != len(cb.VarcharData.Sizes) {
				t.Errorf("varchar array sizes do not match, expected %v got %v", rowz.RowCount, len(cb.VarcharData.Sizes))
			}
		case "TIMESTAMP":
			if int(rowz.RowCount) != len(cb.TimestampData.Timestamps) {
				t.Errorf("timestamp array sizes do not match, expected %v got %v", rowz.RowCount, len(cb.TimestampData.Timestamps))
			}
		case "BOOLEAN":
			if int(rowz.RowCount) != len(cb.BooleanData.Booleans) {
				t.Errorf("boolean array sizes do not match, expected %v got %v", rowz.RowCount, len(cb.BooleanData.Booleans))
			}
		}
	}
}
