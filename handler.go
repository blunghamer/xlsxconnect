package presto

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net"

	"github.com/tealeg/xlsx/v3"
)

// ColTyp is an int type for storing metadata about the data type in the column group
type ColTyp int

// These are the cell types from the ST_CellType spec
const (
	ColTypVarchar ColTyp = iota
	ColTypDouble
	ColTypTimestamp
	ColTypBoolean
)

var errStorage error
var errSignalDataTypeAnalysisDone error
var errNotImplemented error

func init() {
	errStorage = errors.New("Unable to find data in backend")
	errSignalDataTypeAnalysisDone = errors.New("Done evaluating datatypes in 1000 rows")
	errNotImplemented = errors.New("Not implemented")
}

// BlobStoreSplit is mashalled payload of a split in the blobstore we use this to avoid caching in the XLSX layer
type BlobStoreSplit struct {
	Schema string
	Table  string
	File   string
	Sheet  string
}

// CacheEntry stored value per split currently not used
type CacheEntry struct {
	SchemaName string
	TableName  string
	FileName   string
	SheetName  string
}

// Handler has the functions
type Handler struct {
	BaseDir    string
	SplitCache map[[16]byte]CacheEntry
}

// NewHandler based on directory which ist still an option
func NewHandler(folderName string) *Handler {
	return &Handler{BaseDir: folderName, SplitCache: make(map[[16]byte]CacheEntry, 0)}
}

// PrestoListSchemaNames read all top level declarations
func (h *Handler) PrestoListSchemaNames(ctx context.Context) (r []string, err error) {
	fi, err := ioutil.ReadDir(h.BaseDir)
	if err != nil {
		return nil, err
	}
	r = make([]string, len(fi))
	for idx, f := range fi {
		r[idx] = f.Name()
	}
	return r, nil
}

// PrestoListTables list tables in schema or globally
func (h *Handler) PrestoListTables(ctx context.Context, schemaNameOrNull *PrestoThriftNullableSchemaName) (r []*PrestoThriftSchemaTableName, err error) {
	// well, we do some hiding here, we do want a schemaName otherwise we enumerate the whole blob store which is... not good!
	if schemaNameOrNull == nil || schemaNameOrNull.SchemaName == nil {
		return make([]*PrestoThriftSchemaTableName, 0), nil
	}
	fi, err := ioutil.ReadDir(h.BaseDir + "/" + *schemaNameOrNull.SchemaName)
	if err != nil {
		return nil, err
	}

	r = make([]*PrestoThriftSchemaTableName, len(fi))
	for idx, f := range fi {
		r[idx] = &PrestoThriftSchemaTableName{SchemaName: *schemaNameOrNull.SchemaName, TableName: f.Name()}
	}
	return r, nil
}

// should there be a metadata vistor and an extraction visitor?
// both have interleaving logic scattered throught the methods
type visit struct {
	MaxRow    int
	Row       int
	Col       int
	ColNames  []string
	ColTyps   []ColTyp
	Buffers   []*bytes.Buffer
	TableMeta *PrestoThriftTableMetadata
	Records   []*PrestoThriftBlock
}

func newvisit() *visit {
	return &visit{
		MaxRow:    0,
		Row:       0,
		Col:       0,
		ColNames:  make([]string, 0),
		ColTyps:   make([]ColTyp, 0),
		Buffers:   make([]*bytes.Buffer, 0),
		TableMeta: NewPrestoThriftTableMetadata(),
		Records:   make([]*PrestoThriftBlock, 0),
	}
}

func (v *visit) headerExtractor(c *xlsx.Cell) error {
	v.ColNames = append(v.ColNames, c.String())
	return nil
}

func (v *visit) cellVisitorMetadata(c *xlsx.Cell) error {

	// this is overly simplistic, we probably have to guess column types
	// and revert to string if the column shows non uniform colums but we
	// should interrogate at least n rows to have a majority vote on the column
	// types
	if v.Row == 1 {
		ct := NewPrestoThriftColumnMetadata()
		ct.Name = v.ColNames[v.Col]
		switch c.Type() {
		case xlsx.CellTypeString:
			ct.Type = "VARCHAR"
		case xlsx.CellTypeBool:
			ct.Type = "BOOLEAN"
		case xlsx.CellTypeNumeric:
			if c.IsTime() {
				ct.Type = "TIMESTAMP"
			} else {
				ct.Type = "DOUBLE"
			}
		}
		com := "Excel list source from blob storage"
		ct.Comment = &com
		v.TableMeta.Columns[v.Col] = ct
	}

	/*
		log.Printf("Cell %+v", c)
		fv, err := c.FormattedValue()
		if err != nil {
			panic(err)
		}

		fmt.Println("\tNumeric cell?:", c.Type() == xlsx.CellTypeNumeric)
		fmt.Println("\tTimestamp?", c.IsTime())
		fmt.Println("\tString:", c.String())
		fmt.Println("\tFormatted:", fv)
		fmt.Println("\tFormula:", c.Formula())
		fmt.Println("\tGet number format", c.GetNumberFormat())

		// do we need to add records
		if len(v.ColData) < len(v.ColNames) {

		}
	*/
	v.Col++
	return nil
}

func (v *visit) cellVisitorExtractor(c *xlsx.Cell) error {
	initSize := v.MaxRow
	// log.Println("Cellval", c.String())
	if v.Row == 1 {
		switch c.Type() {
		case xlsx.CellTypeString:
			ptv := &PrestoThriftVarchar{}
			ptv.Nulls = make([]bool, initSize)
			ptv.Sizes = make([]int32, initSize)
			ptv.Bytes = make([]byte, initSize)
			v.Buffers[v.Col] = new(bytes.Buffer) //bytes.NewBuffer() i mean we could choose to push in a default size / estimated sized buffer but well....
			v.Records[v.Col] = &PrestoThriftBlock{VarcharData: ptv}
			v.ColTyps[v.Col] = ColTypVarchar
		case xlsx.CellTypeBool:
			ptv := &PrestoThriftBoolean{}
			ptv.Nulls = make([]bool, initSize)
			v.Records[v.Col] = &PrestoThriftBlock{BooleanData: ptv}
			v.ColTyps[v.Col] = ColTypBoolean
		case xlsx.CellTypeNumeric:
			if c.IsTime() {
				ptv := &PrestoThriftTimestamp{}
				ptv.Nulls = make([]bool, initSize)
				ptv.Timestamps = make([]int64, initSize)
				v.Records[v.Col] = &PrestoThriftBlock{TimestampData: ptv}
				v.ColTyps[v.Col] = ColTypTimestamp
			} else {
				ptv := &PrestoThriftDouble{}
				ptv.Nulls = make([]bool, initSize)
				ptv.Doubles = make([]float64, initSize)
				v.Records[v.Col] = &PrestoThriftBlock{DoubleData: ptv}
				v.ColTyps[v.Col] = ColTypDouble
			}
		}
	}

	idRow := v.Row - 1
	switch v.ColTyps[v.Col] {
	case ColTypVarchar:
		// even if this is UTF8 len will cound the bytes, this is intended, to we get the cut points in the byte array across to presto
		s := c.String()
		sLen := int32(len(s))
		// build up buffer, let go handle the increasing in capacity etc.
		v.Buffers[v.Col].Write([]byte(s))
		v.Records[v.Col].VarcharData.Sizes[idRow] = sLen
		v.Records[v.Col].VarcharData.Nulls[idRow] = sLen == 0
	case ColTypDouble:
		t, err := c.Float()
		if err != nil {
			return err
		}
		v.Records[v.Col].DoubleData.Doubles[idRow] = t
		v.Records[v.Col].DoubleData.Nulls[idRow] = false
	case ColTypTimestamp:
		t, err := c.GetTime(false)
		if err != nil {
			return err
		}
		v.Records[v.Col].TimestampData.Timestamps[idRow] = t.Unix() * 1000 // second precision, have to return millis to presto so millis since epoch 1970-01-01
		v.Records[v.Col].TimestampData.Nulls[idRow] = false
	case ColTypBoolean:
		v.Records[v.Col].BooleanData.Booleans[idRow] = c.Bool()
		v.Records[v.Col].BooleanData.Nulls[idRow] = false
	}

	v.Col++
	return nil
}

func (v *visit) rowVisitorExtractor(r *xlsx.Row) error {
	v.Col = 0
	if v.Row == 0 {
		r.ForEachCell(v.headerExtractor)
		v.Records = make([]*PrestoThriftBlock, len(v.ColNames))
		v.ColTyps = make([]ColTyp, len(v.ColNames))
		v.Buffers = make([]*bytes.Buffer, len(v.ColNames))
	} else {
		r.ForEachCell(v.cellVisitorExtractor)
	}
	v.Row++
	return nil
}

func (v *visit) rowVisitorMetadata(r *xlsx.Row) error {
	v.Col = 0
	if v.Row == 0 {
		r.ForEachCell(v.headerExtractor)
		v.TableMeta.Columns = make([]*PrestoThriftColumnMetadata, len(v.ColNames))
		// v.Records.ColumnBlocks = make([]*PrestoThriftBlock, len(v.ColNames))
	} else {
		r.ForEachCell(v.cellVisitorMetadata)
		if v.Row > 1000 {
			return errSignalDataTypeAnalysisDone
		}
	}

	v.Row++
	return nil
}

func assemblePath(baseDir, schema, table string) string {
	return baseDir + "/" + schema + "/" + table
}

func getExcelFile(baseDir, schema, table, file string) (*xlsx.File, error) {
	path := assemblePath(baseDir, schema, table)

	fi, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	if len(fi) == 0 {
		log.Println("No data found in backend path ", path)
		return nil, errStorage
	}

	eh, err := xlsx.OpenFile(path + "/" + file)
	if err != nil {
		log.Println("Unable to read excel file", err)
		return nil, err
	}

	if len(eh.Sheets) == 0 {
		return nil, err
	}

	return eh, nil
}

// PrestoGetTableMetadata derive presto schema from files
func (h *Handler) PrestoGetTableMetadata(ctx context.Context, schemaTableName *PrestoThriftSchemaTableName) (r *PrestoThriftNullableTableMetadata, err error) {

	eh, err := getExcelFile(h.BaseDir, schemaTableName.SchemaName, schemaTableName.TableName, "testfile.xlsx")
	if err != nil {
		log.Println("Unable to read excel file", err)
		return nil, err
	}

	if len(eh.Sheets) == 0 {
		return nil, err
	}

	sh := eh.Sheets[0]

	v := newvisit()
	err = sh.ForEachRow(v.rowVisitorMetadata)
	if err != nil {
		log.Println("Error examining file structure", err)
	}

	r = &PrestoThriftNullableTableMetadata{}
	r.TableMetadata = v.TableMeta
	r.TableMetadata.SchemaTableName = schemaTableName
	r.TableMetadata.SchemaTableName = &PrestoThriftSchemaTableName{SchemaName: schemaTableName.SchemaName, TableName: schemaTableName.TableName}
	log.Println(r.TableMetadata)
	return r, nil
}

// GetOutboundIP Get preferred outbound ip of this machine
func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

// PrestoGetSplits we can advise presto that more is available hmm interesting ...
func (h *Handler) PrestoGetSplits(ctx context.Context, schemaTableName *PrestoThriftSchemaTableName, desiredColumns *PrestoThriftNullableColumnSet, outputConstraint *PrestoThriftTupleDomain, maxSplitCount int32, nextToken *PrestoThriftNullableToken) (r *PrestoThriftSplitBatch, err error) {

	r = &PrestoThriftSplitBatch{}

	// we do not know how to work with output constraints, those are seemingly quite tough to implement. We would have to apply filters and such
	// if we have multiple files in the folder, we could issue splits for each file, right?
	path := assemblePath(h.BaseDir, schemaTableName.SchemaName, schemaTableName.TableName)

	finfos, err := ioutil.ReadDir(path)
	if err != nil {
		log.Println("Error during table blob scan", err)
		return r, err
	}

	if len(finfos) == 0 {
		log.Println("No data found in backend path ", path)
		return r, errStorage
	}

	r.Splits = make([]*PrestoThriftSplit, len(finfos))

	// this should be handled by round robin dns / k8s dns so we expect only one entry anyhow
	hadd := &PrestoThriftHostAddress{Port: 9090, Host: GetOutboundIP().String()}
	hosts := make([]*PrestoThriftHostAddress, 1)
	hosts[0] = hadd

	for idx, fi := range finfos {

		// we have more files but presto does not want moren than maxSplitCount (?)
		// we have to do something with nextToken if there is more to it than that
		// which would mean that we have to consistently enumerate objects in one partition of the blob (right?)
		if int32(idx) >= maxSplitCount {
			continue
		}

		sp := BlobStoreSplit{Schema: schemaTableName.SchemaName, Table: schemaTableName.TableName, File: fi.Name(), Sheet: "Sheet1"}
		byts, err := json.Marshal(sp)
		if err != nil {
			log.Println("Error during serialisation of BlobStoreSplit")
		}

		split := NewPrestoThriftSplit()

		id := &PrestoThriftId{ID: byts}

		split.SplitId = id
		split.Hosts = hosts

		r.Splits[idx] = split
		// we do not really need it so far
		// hmd5 := md5.Sum([]byte("testdata.xlsx"))
		// h.SplitCache[hmd5] = CacheEntry{SchemaName: schemaTableName.SchemaName, TableName: schemaTableName.TableName, FileName: "testdata.xlsx", SheetName: "Sheet1"}
	}
	return r, nil
}

// PrestoGetIndexSplits we can advise presto on index lookups however in excel, that is not really possible, is it?
// we do not want to create a database here, this should be presto, downside is, presto needs to read all data in the excel backend files
func (h *Handler) PrestoGetIndexSplits(ctx context.Context, schemaTableName *PrestoThriftSchemaTableName, indexColumnNames []string, outputColumnNames []string, keys *PrestoThriftPageResult_, outputConstraint *PrestoThriftTupleDomain, maxSplitCount int32, nextToken *PrestoThriftNullableToken) (r *PrestoThriftSplitBatch, err error) {
	// interesting how we should correctly use this one...
	r = &PrestoThriftSplitBatch{}
	return r, nil
}

// PrestoGetRows returns correctly formatted data from the backing store
func (h *Handler) PrestoGetRows(ctx context.Context, splitID *PrestoThriftId, columns []string, maxBytes int64, nextToken *PrestoThriftNullableToken) (r *PrestoThriftPageResult_, err error) {

	r = &PrestoThriftPageResult_{}

	// TODO: connector should honor maxBytes, nextToken, columns correctly...
	// next token unused...
	// maxBytes unused ...
	// columns not used at all

	log.Println("Got", columns, maxBytes, nextToken)

	if columns == nil {
		return r, errNotImplemented
	}

	if len(columns) == 0 {
		return r, errNotImplemented
	}

	var bbs BlobStoreSplit
	err = json.Unmarshal(splitID.ID, &bbs)
	if err != nil {
		return r, err
	}

	// or to keep it idempotent do encode everything int he split id (it is a byte slice after all so we use that to avoid caching)
	/*
		hmd5 := md5.Sum(splitID.ID)
		ce, ok := h.SplitCache[hmd5]
		if !ok {
			return r, errNotImplemented
		}
	*/

	eh, err := getExcelFile(h.BaseDir, bbs.Schema, bbs.Table, bbs.File)
	if err != nil {
		log.Println("Unable to read excel file", err)
		return r, err
	}

	if len(eh.Sheets) == 0 {
		log.Println("Unable to read excel file with no sheets")
		return r, err
	}

	sh, ok := eh.Sheet[bbs.Sheet]
	if !ok {
		log.Println("sheetname not found", bbs.Sheet)
		return r, err
	}

	v := newvisit()
	v.MaxRow = sh.MaxRow
	// we do our own housekeeping of extracted rows and cols anyway
	err = sh.ForEachRow(v.rowVisitorExtractor, xlsx.SkipEmptyRows)
	if err != nil {
		log.Println("Error examining file structure", err)
		return r, err
	}

	extractedRows := int32(v.Row) - 1

	// lets not forget to set the bytebuffers to the Varchar columns
	// an reset the array to less size
	for idx, ctyp := range v.ColTyps {
		switch ctyp {
		case ColTypVarchar:
			v.Records[idx].VarcharData.Bytes = v.Buffers[idx].Bytes()
			v.Records[idx].VarcharData.Sizes = v.Records[idx].VarcharData.Sizes[0:extractedRows]
			v.Records[idx].VarcharData.Nulls = v.Records[idx].VarcharData.Nulls[0:extractedRows]
		case ColTypBoolean:
			v.Records[idx].BooleanData.Booleans = v.Records[idx].BooleanData.Booleans[0:extractedRows]
			v.Records[idx].BooleanData.Nulls = v.Records[idx].BooleanData.Nulls[0:extractedRows]
		case ColTypDouble:
			v.Records[idx].DoubleData.Doubles = v.Records[idx].DoubleData.Doubles[0:extractedRows]
			v.Records[idx].DoubleData.Nulls = v.Records[idx].DoubleData.Nulls[0:extractedRows]
		case ColTypTimestamp:
			v.Records[idx].TimestampData.Timestamps = v.Records[idx].TimestampData.Timestamps[0:extractedRows]
			v.Records[idx].TimestampData.Nulls = v.Records[idx].TimestampData.Nulls[0:extractedRows]
		}
	}

	r = &PrestoThriftPageResult_{}
	r.ColumnBlocks = v.Records
	r.RowCount = extractedRows

	log.Println(v.ColNames, r.RowCount)

	return r, nil
}
