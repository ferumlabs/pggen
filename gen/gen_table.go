package gen

import (
	"fmt"
	"io"
	"text/template"

	"github.com/ferumlabs/pggen/gen/internal/config"
	"github.com/ferumlabs/pggen/gen/internal/meta"
)

// Generate code for all of the tables
func (g *Generator) genTables(into io.Writer, tables []config.TableConfig) error {
	if len(tables) > 0 {
		g.log.Infof("	generating %d tables\n", len(tables))
	} else {
		return nil
	}

	g.imports[`"database/sql"`] = true
	g.imports[`"context"`] = true
	g.imports[`"fmt"`] = true
	g.imports[`"strings"`] = true
	g.imports[`"sync"`] = true
	g.imports[`"github.com/ethanpailes/pgtypes"`] = true
	g.imports[`"github.com/ferumlabs/pggen/include"`] = true
	g.imports[`"github.com/ferumlabs/pggen/unstable"`] = true
	g.imports[`"github.com/ferumlabs/pggen"`] = true
	g.imports[`"ferum/utils/batcher"`] = true

	for i := range tables {
		err := g.genTable(into, &tables[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func tableGenCtxFromInfo(info *meta.TableMeta) meta.TableGenCtx {
	return meta.TableGenCtx{
		PgName:         info.Info.PgName,
		GoName:         info.Info.GoName,
		PkeyCol:        info.Info.PkeyCol,
		PkeyColIdx:     info.Info.PkeyColIdx,
		AllIncludeSpec: info.AllIncludeSpec.String(),
		Meta:           info,
	}
}

func (g *Generator) genTable(
	into io.Writer,
	table *config.TableConfig,
) (err error) {
	g.log.Infof("		generating table '%s'\n", table.Name)
	defer func() {
		if err != nil {
			err = fmt.Errorf(
				"while generating table '%s': %s", table.Name, err.Error())
		}
	}()

	tableInfo, ok := g.metaResolver.TableMeta(table.Name)
	if !ok {
		return fmt.Errorf("could not get schema info about table '%s'", table.Name)
	}

	genCtx := tableGenCtxFromInfo(tableInfo)
	if genCtx.PkeyCol == nil {
		err = fmt.Errorf("no primary key for table")
		return
	}

	if tableInfo.HasUpdatedAtField || tableInfo.HasCreatedAtField {
		g.imports[`"time"`] = true
	}

	err = g.typeResolver.EmitStructType(genCtx.GoName, genCtx)
	if err != nil {
		return
	}

	return tableShimTmpl.Execute(into, genCtx)
}

var tableShimTmpl *template.Template = template.Must(template.New("table-shim-tmpl").Parse(`

func (p *PGClient) Get{{ .GoName }}(
	ctx context.Context,
	id {{ .PkeyCol.TypeInfo.Name }},
	opts ...pggen.GetOpt,
) ({{- if .Meta.Config.BoxResults }}*{{- end }}{{ .GoName }}, error) {
	return p.impl.get{{ .GoName }}(ctx, id)
}
func (tx *TxPGClient) Get{{ .GoName }}(
	ctx context.Context,
	id {{ .PkeyCol.TypeInfo.Name }},
	opts ...pggen.GetOpt,
) ({{- if .Meta.Config.BoxResults }}*{{- end }}{{ .GoName }}, error) {
	return tx.impl.get{{ .GoName }}(ctx, id)
}
func (conn *ConnPGClient) Get{{ .GoName }}(
	ctx context.Context,
	id {{ .PkeyCol.TypeInfo.Name }},
	opts ...pggen.GetOpt,
) ({{- if .Meta.Config.BoxResults }}*{{- end }}{{ .GoName }}, error) {
	return conn.impl.get{{ .GoName }}(ctx, id)
}
func (p *pgClientImpl) get{{ .GoName }}(
	ctx context.Context,
	id {{ .PkeyCol.TypeInfo.Name }},
	opts ...pggen.GetOpt,
) ({{- if .Meta.Config.BoxResults }}*{{- end }}{{ .GoName }}, error) {
	values, err := p.list{{ .GoName }}(ctx, []{{ .PkeyCol.TypeInfo.Name }}{id}, true /* isGet */)
	if err != nil {
		return {{ if .Meta.Config.BoxResults }}nil{{- else }}{{ .GoName }}{}{{- end }}, err
	}

	// List{{ .GoName }} always returns the same number of records as were
	// requested, so this is safe.
	return {{ if .Meta.Config.BoxResults }}&{{- end }}values[0], err
}

func (p *PGClient) List{{ .GoName }}(
	ctx context.Context,
	ids []{{ .PkeyCol.TypeInfo.Name }},
	opts ...pggen.ListOpt,
) (ret []{{- if .Meta.Config.BoxResults }}*{{- end }}{{ .GoName }}, err error) {
	return p.impl.list{{ .GoName }}(ctx, ids, false /* isGet */, opts...)
}
func (tx *TxPGClient) List{{ .GoName }}(
	ctx context.Context,
	ids []{{ .PkeyCol.TypeInfo.Name }},
	opts ...pggen.ListOpt,
) (ret []{{- if .Meta.Config.BoxResults }}*{{- end }}{{ .GoName }}, err error) {
	return tx.impl.list{{ .GoName }}(ctx, ids, false /* isGet */, opts...)
}
func (conn *ConnPGClient) List{{ .GoName }}(
	ctx context.Context,
	ids []{{ .PkeyCol.TypeInfo.Name }},
	opts ...pggen.ListOpt,
) (ret []{{- if .Meta.Config.BoxResults }}*{{- end }}{{ .GoName }}, err error) {
	return conn.impl.list{{ .GoName }}(ctx, ids, false /* isGet */, opts...)
}
func (p *pgClientImpl) list{{ .GoName }}(
	ctx context.Context,
	ids []{{ .PkeyCol.TypeInfo.Name }},
	isGet bool,
	opts ...pggen.ListOpt,
) ([]{{- if .Meta.Config.BoxResults }}*{{- end }}{{ .GoName }}, error) {
	opt := pggen.ListOptions{}
	for _, o := range opts {
		o(&opt)
	}
	if len(ids) == 0 {
		return []{{- if .Meta.Config.BoxResults }}*{{- end }}{{ .GoName }}{}, nil
	}
	
	ret := make([]{{- if .Meta.Config.BoxResults }}*{{- end }}{{ .GoName }}, 0, len(ids))
	batches := batcher.Batch(ids, BatchSize)
	for _, batch := range batches {
		batchRet, err := p.listBatch{{ .GoName }}(ctx, batch, isGet, opt)
		if err != nil {
			return nil, err
		}
		ret = append(ret, batchRet...) 
	}

	return ret, nil 
}
func (p *pgClientImpl) listBatch{{ .GoName }}(
	ctx context.Context,
	ids []{{ .PkeyCol.TypeInfo.Name }},
	isGet bool,
	opt pggen.ListOptions,
) ([]{{- if .Meta.Config.BoxResults }}*{{- end }}{{ .GoName }}, error) {
	if len(ids) == 0 {
		return []{{- if .Meta.Config.BoxResults }}*{{- end }}{{ .GoName }}{}, nil
	}

	rows, err := p.queryContext(
		ctx,
		` + "`" + `SELECT {{ range $i, $col := .Meta.Info.Cols }}{{ if $i }},{{ end }}"{{ $col.PgName }}"{{ end }} FROM {{ .PgName }} WHERE "{{ .PkeyCol.PgName }}" = ANY($1)
		{{- if .Meta.HasDeletedAtField }} AND "{{ .Meta.PgDeletedAtField }}" IS NULL {{ end }}` + "`" + `,
		pgtypes.Array(ids),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ret := make([]{{- if .Meta.Config.BoxResults }}*{{- end }}{{ .GoName }}, 0, len(ids))
	for rows.Next() {
		var value {{ .GoName }}
		err = value.Scan(rows)
		if err != nil {
			return nil, err
		}
		ret = append(ret, {{- if .Meta.Config.BoxResults }}&{{- end }}value)
	}

	if len(ret) != len(ids) {
		if isGet {
			return nil, &unstable.NotFoundError{
				Msg: "Get{{ .GoName }}: record not found",
			}
		} else if !opt.SucceedOnPartialResults {
			return nil, &unstable.NotFoundError{
				Msg: fmt.Sprintf(
					"List{{ .GoName }}: asked for %d records, found %d",
					len(ids),
					len(ret),
				),
			}
		}
	}

	return ret, nil
}

// Insert a {{ .GoName }} into the database. Returns the primary
// key of the inserted row.
func (p *PGClient) Insert{{ .GoName }}(
	ctx context.Context,
	value {{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }},
	opts ...pggen.InsertOpt,
) (ret {{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, err error) {
	return p.impl.insert{{ .GoName }}(ctx, value, opts...)
}
// Insert a {{ .GoName }} into the database. Returns the primary
// key of the inserted row.
func (tx *TxPGClient) Insert{{ .GoName }}(
	ctx context.Context,
	value {{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }},
	opts ...pggen.InsertOpt,
) (ret {{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, err error) {
	return tx.impl.insert{{ .GoName }}(ctx, value, opts...)
}
// Insert a {{ .GoName }} into the database. Returns the primary
// key of the inserted row.
func (conn *ConnPGClient) Insert{{ .GoName }}(
	ctx context.Context,
	value {{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }},
	opts ...pggen.InsertOpt,
) (ret {{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, err error) {
	return conn.impl.insert{{ .GoName }}(ctx, value, opts...)
}
// Insert a {{ .GoName }} into the database. Returns the primary
// key of the inserted row.
func (p *pgClientImpl) insert{{ .GoName }}(
	ctx context.Context,
	value {{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }},
	opts ...pggen.InsertOpt,
) (ret {{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, err error) {
	var rets []{{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}
	rets, err = p.bulkInsert{{ .GoName }}(ctx, []{{ .GoName }}{value}, opts...)
	if err != nil {
		return ret, err
	}

	if len(rets) != 1 {
		return ret, fmt.Errorf("inserting a {{ .GoName }}: %d rows (expected 1)", len(rets))
	}

	ret = rets[0]
	return
}

// Insert a list of {{ .GoName }}. Returns a list of the primary keys of
// the inserted rows.
func (p *PGClient) BulkInsert{{ .GoName }}(
	ctx context.Context,
	values []{{ .GoName }},
	opts ...pggen.InsertOpt,
) ([]{{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, error) {
	return p.impl.bulkInsert{{ .GoName }}(ctx, values, opts...)
}
// Insert a list of {{ .GoName }}. Returns a list of the primary keys of
// the inserted rows.
func (tx *TxPGClient) BulkInsert{{ .GoName }}(
	ctx context.Context,
	values []{{ .GoName }},
	opts ...pggen.InsertOpt,
) ([]{{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, error) {
	return tx.impl.bulkInsert{{ .GoName }}(ctx, values, opts...)
}
// Insert a list of {{ .GoName }}. Returns a list of the primary keys of
// the inserted rows.
func (conn *ConnPGClient) BulkInsert{{ .GoName }}(
	ctx context.Context,
	values []{{ .GoName }},
	opts ...pggen.InsertOpt,
) ([]{{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, error) {
	return conn.impl.bulkInsert{{ .GoName }}(ctx, values, opts...)
}
// Insert a list of {{ .GoName }}. Returns a list of the primary keys of
// the inserted rows.
func (p *pgClientImpl) bulkInsert{{ .GoName }}(
	ctx context.Context,
	values []{{ .GoName }},
	opts ...pggen.InsertOpt,
) ([]{{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, error) {
	if len(values) == 0 {
		return []{{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}{}, nil
	}

	opt := pggen.InsertOptions{}
	for _, o := range opts {
		o(&opt)
	}
	
	rets := make([]{{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, 0, len(values))

	batches := batcher.Batch(values, BatchSize)
	for _, batch := range batches {
		batchRet, err := p.bulkInsertBatch{{ .GoName }}(ctx, batch, opt)
		if err != nil {
			return nil, err
		}
		rets = append(rets, batchRet...)
	}

	return rets, nil
}
func (p *pgClientImpl) bulkInsertBatch{{ .GoName }}(
	ctx context.Context,
	values []{{ .GoName }},
	opt pggen.InsertOptions,
) ([]{{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, error) {
	if len(values) == 0 {
		return []{{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}{}, nil
	}

	{{- if (or .Meta.HasCreatedAtField .Meta.HasUpdatedAtField) }}
	if !opt.DisableTimestamps {
		now := time.Now()

		{{- if .Meta.HasCreatedAtField }}
		for i := range values {
			{{- if .Meta.CreatedAtHasTimezone }}
			createdAt := now
			{{- else }}
			createdAt := now.UTC()
			{{- end }}
	
			{{- if .Meta.HasCreatedAtField }}
			{{- if .Meta.CreatedAtFieldIsNullable }}
			values[i].{{ .Meta.GoCreatedAtField }} = &createdAt
			{{- else }}
			values[i].{{ .Meta.GoCreatedAtField }} = createdAt
			{{- end }}
			{{- end }}
		}
		{{- end }}
	
		{{- if .Meta.HasUpdatedAtField }}
		for i := range values {
			{{- if .Meta.UpdatedAtHasTimezone }}
			updatedAt := now
			{{- else }}
			updatedAt := now.UTC()
			{{- end }}
	
			{{- if .Meta.HasUpdatedAtField }}
			{{- if .Meta.UpdatedAtFieldIsNullable }}
			values[i].{{ .Meta.GoUpdatedAtField }} = &updatedAt
			{{- else }}
			values[i].{{ .Meta.GoUpdatedAtField }} = updatedAt
			{{- end }}
			{{- end }}
		}
		{{- end }}
	}
	{{- end }}

	defaultFields := opt.DefaultFields.Intersection(defaultableColsFor{{ .GoName }})
	args := make([]interface{}, 0, {{ len .Meta.Info.Cols }} * len(values))
	for _, v := range values {
		{{- range .Meta.Info.Cols }}
		{{- if (not .IsPrimary) }}
		{{- if .Nullable }}
		if !defaultFields.Test({{ $.GoName }}{{ .GoName }}FieldIndex) {
			args = append(args, {{ call .TypeInfo.NullSqlArgument (printf "v.%s" .GoName) }})
		}
		{{- else }}
		if !defaultFields.Test({{ $.GoName }}{{ .GoName }}FieldIndex) {
			args = append(args, {{ call .TypeInfo.SqlArgument (printf "v.%s" .GoName) }})
		}
		{{- end }}
		{{- else }}
		if !defaultFields.Test({{ $.GoName }}{{ .GoName }}FieldIndex) {
			{{- if .Nullable }}
			args = append(args, {{ call .TypeInfo.NullSqlArgument (printf "v.%s" .GoName) }})
			{{- else }}
			args = append(args, {{ call .TypeInfo.SqlArgument (printf "v.%s" .GoName) }})
			{{- end }}
		}
		{{- end }}
		{{- end }}
	}

	bulkInsertQuery := genBulkInsertStmt(
		` + "`" + `{{ .PgName }}` + "`" + `,
		fieldsFor{{ .GoName }},
		len(values),
		"{{ .PkeyCol.PgName }}",
		true,
		defaultFields,
	)

	rows, err := p.queryContext(ctx, bulkInsertQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rets := make([]{{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, 0, len(values))
	for rows.Next() {
		var ret {{ .GoName }}
		err = ret.Scan(rows)
		if err != nil {
			return nil, err
		}
		rets = append(rets, {{ if .Meta.Config.BoxResults }}&{{ end }}ret)
	}

	return rets, nil
}

// bit indicies for 'fieldMask' parameters
const (
	{{- range $i, $c := .Meta.Info.Cols }}
	{{ $.GoName }}{{ $c.GoName }}FieldIndex int = {{ $i }}
	{{- end }}
	{{ $.GoName }}MaxFieldIndex int = ({{ len .Meta.Info.Cols }} - 1)
)

// A field set saying that all fields in {{ .GoName }} should be updated.
// For use as a 'fieldMask' parameter
var {{ .GoName }}AllFields pggen.FieldSet = pggen.NewFieldSetFilled({{ len .Meta.Info.Cols }})

// A field set containing all mutable fields for {{ .GoName }}.
// For use as a 'fieldMask' parameter
var {{ .GoName }}MutableFields pggen.FieldSet = pggen.NewFieldSet({{ len .Meta.Info.Cols }})
{{- range .Meta.Info.Cols }}
{{- if .IsMutable }}.
Set({{ $.GoName }}{{ .GoName }}FieldIndex, true)
{{- end }}
{{- end }}

var defaultableColsFor{{ .GoName }} = func() pggen.FieldSet {
	fs := pggen.NewFieldSet({{ .GoName }}MaxFieldIndex)
	{{- range .Meta.Info.Cols }}
	{{- if .DefaultExpr }}
	fs.Set({{ $.GoName }}{{ .GoName }}FieldIndex, true)
	{{- end }}
	{{- end }}
	return fs
}()

var fieldsFor{{ .GoName }} []fieldNameAndIdx = []fieldNameAndIdx{
	{{- range .Meta.Info.Cols }}
	{ name: ` + "`" + `{{ .PgName }}` + "`" + `, idx: {{ $.GoName }}{{ .GoName }}FieldIndex },
	{{- end }}
}

// Update a {{ .GoName }}. 'value' must at the least have
// a primary key set. The 'fieldMask' field set indicates which fields
// should be updated in the database.
//
// Returns the primary key of the updated row.
func (p *PGClient) Update{{ .GoName }}(
	ctx context.Context,
	value {{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }},
	fieldMask pggen.FieldSet,
	opts ...pggen.UpdateOpt,
) (ret {{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, err error) {
	return p.impl.update{{ .GoName }}(ctx, value, fieldMask, opts...)
}
// Update a {{ .GoName }}. 'value' must at the least have
// a primary key set. The 'fieldMask' field set indicates which fields
// should be updated in the database.
//
// Returns the primary key of the updated row.
func (tx *TxPGClient) Update{{ .GoName }}(
	ctx context.Context,
	value {{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }},
	fieldMask pggen.FieldSet,
	opts ...pggen.UpdateOpt,
) (ret {{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, err error) {
	return tx.impl.update{{ .GoName }}(ctx, value, fieldMask, opts...)
}
// Update a {{ .GoName }}. 'value' must at the least have
// a primary key set. The 'fieldMask' field set indicates which fields
// should be updated in the database.
//
// Returns the primary key of the updated row.
func (conn *ConnPGClient) Update{{ .GoName }}(
	ctx context.Context,
	value {{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }},
	fieldMask pggen.FieldSet,
	opts ...pggen.UpdateOpt,
) (ret {{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, err error) {
	return conn.impl.update{{ .GoName }}(ctx, value, fieldMask, opts...)
}
func (p *pgClientImpl) update{{ .GoName }}(
	ctx context.Context,
	value {{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }},
	fieldMask pggen.FieldSet,
	opts ...pggen.UpdateOpt,
) ({{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, error) {
	var ret {{ .GoName }}

	opt := pggen.UpdateOptions{}
	for _, o := range opts {
		o(&opt)
	}	

	if !fieldMask.Test({{ .GoName }}{{ .PkeyCol.GoName }}FieldIndex) {
		return {{ if .Meta.Config.BoxResults }}&{{ end }}ret, fmt.Errorf(` + "`" + `primary key required for updates to '{{ .PgName }}'` + "`" + `)
	}

	{{- if .Meta.HasUpdatedAtField }}
	if !opt.DisableTimestamps {
		{{- if .Meta.UpdatedAtHasTimezone }}
		now := time.Now()
		{{- else }}
		now := time.Now().UTC()
		{{- end }}
		{{- if .Meta.UpdatedAtFieldIsNullable }}
		value.{{ .Meta.GoUpdatedAtField }} = &now
		{{- else }}
		value.{{ .Meta.GoUpdatedAtField }} = now
		{{- end }}
		fieldMask.Set({{ .GoName }}{{ .Meta.GoUpdatedAtField }}FieldIndex, true)
	}
	{{- end }}

	updateStmt := genUpdateStmt(
		` + "`" + `{{ .PgName }}` + "`" + `,
		"{{ .PkeyCol.PgName }}",
		fieldsFor{{ .GoName }},
		fieldMask,
		"{{ .PkeyCol.PgName }}",
	)

	args := make([]interface{}, 0, {{ len .Meta.Info.Cols }})

	{{- range .Meta.Info.Cols }}
	if fieldMask.Test({{ $.GoName }}{{ .GoName }}FieldIndex) {
		{{- if .Nullable }}
		args = append(args, {{ call .TypeInfo.NullSqlArgument (printf "value.%s" .GoName) }})
		{{- else }}
		args = append(args, {{ call .TypeInfo.SqlArgument (printf "value.%s" .GoName) }})
		{{- end }}
	}
	{{- end }}

	// add the primary key arg for the WHERE condition
	args = append(args, value.{{ .PkeyCol.GoName }})

	rows, err := p.db.QueryContext(ctx, updateStmt, args...)
	if err != nil {
		return {{ if .Meta.Config.BoxResults }}&{{ end }}ret, err
	}
	defer rows.Close()
	rows.Next()
	
	err = ret.Scan(rows)
	if err != nil {
		return {{ if .Meta.Config.BoxResults }}&{{ end }}ret, err
	}

	return {{ if .Meta.Config.BoxResults }}&{{ end }}ret, nil
}

// Upsert a {{ .GoName }} value. If the given value conflicts with
// an existing row in the database, use the provided value to update that row
// rather than inserting it. Only the fields specified by 'fieldMask' are
// actually updated. All other fields are left as-is.
func (p *PGClient) Upsert{{ .GoName }}(
	ctx context.Context,
	value {{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }},
	constraintNames []string,
	fieldMask pggen.FieldSet,
	opts ...pggen.UpsertOpt,
) (ret {{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, err error) {
	var vals []{{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}
	vals, err = p.impl.bulkUpsert{{ .GoName }}(ctx, []{{ .GoName }}{value}, constraintNames, fieldMask, opts...)
	if err != nil {
		return
	}
	if len(vals) == 1 {
		return vals[0], nil
	}

	// only possible if no upsert fields were specified by the field mask
	return ret, nil
}
// Upsert a {{ .GoName }} value. If the given value conflicts with
// an existing row in the database, use the provided value to update that row
// rather than inserting it. Only the fields specified by 'fieldMask' are
// actually updated. All other fields are left as-is.
func (tx *TxPGClient) Upsert{{ .GoName }}(
	ctx context.Context,
	value {{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }},
	constraintNames []string,
	fieldMask pggen.FieldSet,
	opts ...pggen.UpsertOpt,
) (ret {{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, err error) {
	var vals []{{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}
	vals, err = tx.impl.bulkUpsert{{ .GoName }}(ctx, []{{ .GoName }}{value}, constraintNames, fieldMask, opts...)
	if err != nil {
		return
	}
	if len(vals) == 1 {
		return vals[0], nil
	}

	// only possible if no upsert fields were specified by the field mask
	return ret, nil
}
// Upsert a {{ .GoName }} value. If the given value conflicts with
// an existing row in the database, use the provided value to update that row
// rather than inserting it. Only the fields specified by 'fieldMask' are
// actually updated. All other fields are left as-is.
func (conn *ConnPGClient) Upsert{{ .GoName }}(
	ctx context.Context,
	value {{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }},
	constraintNames []string,
	fieldMask pggen.FieldSet,
	opts ...pggen.UpsertOpt,
) (ret {{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, err error) {
	var vals []{{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}
	vals, err = conn.impl.bulkUpsert{{ .GoName }}(ctx, []{{ .GoName }}{value}, constraintNames, fieldMask, opts...)
	if err != nil {
		return
	}
	if len(vals) == 1 {
		return vals[0], nil
	}

	// only possible if no upsert fields were specified by the field mask
	return ret, nil
}


// Upsert a set of {{ .GoName }} values. If any of the given values conflict with
// existing rows in the database, use the provided values to update the rows which
// exist in the database rather than inserting them. Only the fields specified by
// 'fieldMask' are actually updated. All other fields are left as-is.
func (p *PGClient) BulkUpsert{{ .GoName }}(
	ctx context.Context,
	values []{{ .GoName }},
	constraintNames []string,
	fieldMask pggen.FieldSet,
	opts ...pggen.UpsertOpt,
) (ret []{{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, err error) {
	return p.impl.bulkUpsert{{ .GoName }}(ctx, values, constraintNames, fieldMask, opts...)
}
// Upsert a set of {{ .GoName }} values. If any of the given values conflict with
// existing rows in the database, use the provided values to update the rows which
// exist in the database rather than inserting them. Only the fields specified by
// 'fieldMask' are actually updated. All other fields are left as-is.
func (tx *TxPGClient) BulkUpsert{{ .GoName }}(
	ctx context.Context,
	values []{{ .GoName }},
	constraintNames []string,
	fieldMask pggen.FieldSet,
	opts ...pggen.UpsertOpt,
) (ret []{{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, err error) {
	return tx.impl.bulkUpsert{{ .GoName }}(ctx, values, constraintNames, fieldMask, opts...)
}
// Upsert a set of {{ .GoName }} values. If any of the given values conflict with
// existing rows in the database, use the provided values to update the rows which
// exist in the database rather than inserting them. Only the fields specified by
// 'fieldMask' are actually updated. All other fields are left as-is.
func (conn *ConnPGClient) BulkUpsert{{ .GoName }}(
	ctx context.Context,
	values []{{ .GoName }},
	constraintNames []string,
	fieldMask pggen.FieldSet,
	opts ...pggen.UpsertOpt,
) (ret []{{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, err error) {
	return conn.impl.bulkUpsert{{ .GoName }}(ctx, values, constraintNames, fieldMask, opts...)
}
func (p *pgClientImpl) bulkUpsert{{ .GoName }}(
	ctx context.Context,
	values []{{ .GoName }},
	constraintNames []string,
	fieldMask pggen.FieldSet,
	opts ...pggen.UpsertOpt,
) ([]{{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, error) {
	if len(values) == 0 {
		return []{{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}{}, nil
	}

	options := pggen.UpsertOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	if constraintNames == nil || len(constraintNames) == 0 {
		constraintNames = []string{` + "`" + `{{ .PkeyCol.PgName }}` + "`" + `}
	}

	vals := make([]{{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, 0, len(values))
	batches := batcher.Batch(values, BatchSize)
	for _, batch := range batches {
		batchVals, err := p.bulkUpsertBatch{{ .GoName }}(ctx, batch, constraintNames, fieldMask, options)
		if err != nil {
			return nil, err
		}
		vals = append(vals, batchVals...)
	}
	return vals, nil
}
func (p *pgClientImpl) bulkUpsertBatch{{ .GoName }}(
	ctx context.Context,
	values []{{ .GoName }},
	constraintNames []string,
	fieldMask pggen.FieldSet,
	opt pggen.UpsertOptions,
) ([]{{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, error) {
	if len(values) == 0 {
		return []{{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}{}, nil
	}

	{{ if (or .Meta.HasCreatedAtField .Meta.HasUpdatedAtField) }}
	if !options.DisableTimestamps {
		now := time.Now()
	
		{{- if .Meta.HasCreatedAtField }}
		{{- if .Meta.CreatedAtHasTimezone }}
		createdAt := now
		{{- else }}
		createdAt := now.UTC()
		{{- end }}
		for i := range values {
			{{- if .Meta.CreatedAtFieldIsNullable }}
			values[i].{{ .Meta.GoCreatedAtField }} = &createdAt
			{{- else }}
			values[i].{{ .Meta.GoCreatedAtField }} = createdAt
			{{- end }}
		}
		{{- end}}
	
		{{- if .Meta.HasUpdatedAtField }}
		{{- if .Meta.UpdatedAtHasTimezone }}
		updatedAt := now
		{{- else }}
		updatedAt := now.UTC()
		{{- end }}
		for i := range values {
			{{- if .Meta.UpdatedAtFieldIsNullable }}
			values[i].{{ .Meta.GoUpdatedAtField }} = &updatedAt
			{{- else }}
			values[i].{{ .Meta.GoUpdatedAtField }} = updatedAt
			{{- end }}
		}
		fieldMask.Set({{ .GoName }}{{ .Meta.GoUpdatedAtField }}FieldIndex, true)
		{{- end }}
	}
	{{- end }}

	defaultFields := opt.DefaultFields.Intersection(defaultableColsFor{{ .GoName }})
	var stmt strings.Builder
	genInsertCommon(
		&stmt,
		` + "`" + `{{ .PgName }}` + "`" + `,
		fieldsFor{{ .GoName }},
		len(values),
		` + "`" + `{{ .PkeyCol.PgName }}` + "`" + `,
		true,
		defaultFields,
	)

	setBits := fieldMask.CountSetBits()
	hasConflictAction := setBits > 1 ||
		(setBits == 1 && fieldMask.Test({{ .GoName }}{{ .PkeyCol.GoName }}FieldIndex)) ||
		(setBits == 1 && !fieldMask.Test({{ .GoName }}{{ .PkeyCol.GoName }}FieldIndex))

	if hasConflictAction {
		stmt.WriteString("ON CONFLICT (")
		stmt.WriteString(strings.Join(constraintNames, ","))
		stmt.WriteString(") DO UPDATE SET ")

		updateCols := make([]string, 0, {{ len .Meta.Info.Cols }})
		updateExprs := make([]string, 0, {{ len .Meta.Info.Cols }})
		updateCols = append(updateCols, ` + "`" + `{{ .PkeyCol.PgName }}` + "`" + `)
		updateExprs = append(updateExprs, ` + "`" + `excluded.{{ .PkeyCol.PgName }}` + "`" + `)
		{{- range $i, $col := .Meta.Info.Cols }}
		{{- if (not (eq $i $.PkeyColIdx)) }}
		if fieldMask.Test({{ $.GoName }}{{ $col.GoName }}FieldIndex) {
			updateCols = append(updateCols, ` + "`" + `{{ $col.PgName }}` + "`" + `)
			updateExprs = append(updateExprs, ` + "`" + `excluded.{{ $col.PgName }}` + "`" + `)
		}
		{{- end }}
		{{- end }}
		if len(updateCols) > 1 {
			stmt.WriteRune('(')
		}
		stmt.WriteString(strings.Join(updateCols, ","))
		if len(updateCols) > 1 {
			stmt.WriteRune(')')
		}
		stmt.WriteString(" = ")
		if len(updateCols) > 1 {
			stmt.WriteRune('(')
		}
		stmt.WriteString(strings.Join(updateExprs, ","))
		if len(updateCols) > 1 {
			stmt.WriteRune(')')
		}
	} else {
		stmt.WriteString("ON CONFLICT DO NOTHING")
	}

	stmt.WriteString(` + "`" + ` RETURNING *` + "`" + `)

	args := make([]interface{}, 0, {{ len .Meta.Info.Cols }} * len(values))
	for _, v := range values {
		{{- range $i, $col := .Meta.Info.Cols }}
		{{- if (eq $i $.PkeyColIdx) }}
		if !defaultFields.Test({{ $.GoName }}{{ .GoName }}FieldIndex) {
			{{- if .Nullable }}
			args = append(args, {{ call .TypeInfo.NullSqlArgument (printf "v.%s" .GoName) }})
			{{- else }}
			args = append(args, {{ call .TypeInfo.SqlArgument (printf "v.%s" .GoName) }})
			{{- end }}
		}
		{{- else }}
		{{- if .Nullable }}
		if !defaultFields.Test({{ $.GoName }}{{ .GoName }}FieldIndex) {
			args = append(args, {{ call .TypeInfo.NullSqlArgument (printf "v.%s" .GoName) }})
		}
		{{- else }}
		if !defaultFields.Test({{ $.GoName }}{{ .GoName }}FieldIndex) {
			args = append(args, {{ call .TypeInfo.SqlArgument (printf "v.%s" .GoName) }})
		}
		{{- end }}
		{{- end }}
		{{- end }}
	}

	rows, err := p.queryContext(ctx, stmt.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	vals := make([]{{ if .Meta.Config.BoxResults }}*{{ end }}{{ .GoName }}, 0, len(values))
	for rows.Next() {
		var val {{ .GoName }}
		err = val.Scan(rows)
		if err != nil {
			return nil, err
		}
		vals = append(vals, {{ if .Meta.Config.BoxResults }}&{{ end }}val)
	}

	return vals, nil
}

func (p *PGClient) Delete{{ .GoName }}(
	ctx context.Context,
	id {{ .PkeyCol.TypeInfo.Name }},
	opts ...pggen.DeleteOpt,
) error {
	return p.impl.bulkDelete{{ .GoName }}(ctx, []{{ .PkeyCol.TypeInfo.Name }}{id}, opts...)
}
func (tx *TxPGClient) Delete{{ .GoName }}(
	ctx context.Context,
	id {{ .PkeyCol.TypeInfo.Name }},
	opts ...pggen.DeleteOpt,
) error {
	return tx.impl.bulkDelete{{ .GoName }}(ctx, []{{ .PkeyCol.TypeInfo.Name }}{id}, opts...)
}
func (conn *ConnPGClient) Delete{{ .GoName }}(
	ctx context.Context,
	id {{ .PkeyCol.TypeInfo.Name }},
	opts ...pggen.DeleteOpt,
) error {
	return conn.impl.bulkDelete{{ .GoName }}(ctx, []{{ .PkeyCol.TypeInfo.Name }}{id}, opts...)
}

func (p *PGClient) BulkDelete{{ .GoName }}(
	ctx context.Context,
	ids []{{ .PkeyCol.TypeInfo.Name }},
	opts ...pggen.DeleteOpt,
) error {
	return p.impl.bulkDelete{{ .GoName }}(ctx, ids, opts...)
}
func (tx *TxPGClient) BulkDelete{{ .GoName }}(
	ctx context.Context,
	ids []{{ .PkeyCol.TypeInfo.Name }},
	opts ...pggen.DeleteOpt,
) error {
	return tx.impl.bulkDelete{{ .GoName }}(ctx, ids, opts...)
}
func (conn *ConnPGClient) BulkDelete{{ .GoName }}(
	ctx context.Context,
	ids []{{ .PkeyCol.TypeInfo.Name }},
	opts ...pggen.DeleteOpt,
) error {
	return conn.impl.bulkDelete{{ .GoName }}(ctx, ids, opts...)
}
func (p *pgClientImpl) bulkDelete{{ .GoName }}(
	ctx context.Context,
	ids []{{ .PkeyCol.TypeInfo.Name }},
	opts ...pggen.DeleteOpt,
) error {
	if len(ids) == 0 {
		return nil
	}

	options := pggen.DeleteOptions{}
	for _, o := range opts {
		o(&options)
	}

	batches := batcher.Batch(ids, BatchSize)
	for _, batch := range batches {
		err := p.bulkDeleteBatch{{ .GoName }}(ctx, batch, options)
		if err != nil {
			return err
		}
	}

	return nil
}
func (p *pgClientImpl) bulkDeleteBatch{{ .GoName }}(
	ctx context.Context,
	ids []{{ .PkeyCol.TypeInfo.Name }},
	opt pggen.DeleteOptions,
) error {
	if len(ids) == 0 {
		return nil
	}

	{{- if .Meta.HasDeletedAtField }}
	{{- if .Meta.DeletedAtHasTimezone }}
	now := time.Now()
	{{- else }}
	now := time.Now().UTC()
	{{- end }}
	var (
		res sql.Result
		err error
	)
	if options.DoHardDelete {
		res, err = p.db.ExecContext(
			ctx,
			` + "`" + `DELETE FROM {{ .PgName }} WHERE "{{ .PkeyCol.PgName }}" = ANY($1)` + "`" + `,
			pgtypes.Array(ids),
		)
	} else {
		res, err = p.db.ExecContext(
			ctx,
			` + "`" + `UPDATE {{ .PgName }} SET "{{ .Meta.PgDeletedAtField }}" = $1 WHERE "{{ .PkeyCol.PgName }}" = ANY($2)` + "`" + `,
			now,
			pgtypes.Array(ids),
		)
	}
	{{- else }}
	res, err := p.db.ExecContext(
		ctx,
		` + "`" + `DELETE FROM {{ .PgName }} WHERE "{{ .PkeyCol.PgName }}" = ANY($1)` + "`" + `,
		pgtypes.Array(ids),
	)
	{{- end }}
	if err != nil {
		return err
	}

	nrows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if nrows != int64(len(ids)) {
		return fmt.Errorf(
			"BulkDelete{{ .GoName }}: %d rows deleted, expected %d",
			nrows,
			len(ids),
		)
	}

	return err
}

var {{ .GoName }}AllIncludes *include.Spec = include.Must(include.Parse(
	` + "`" + `{{ .AllIncludeSpec }}` + "`" + `,
))

`))
