package gen

import (
	"fmt"
	"io"
	"text/template"

	"github.com/ferumlabs/pggen/gen/internal/config"
	"github.com/ferumlabs/pggen/gen/internal/meta"
)

// genInterfaces emits the DBQueries interface shared between the generated PGClient
// and the generated TxPGClient. This allows user code to be written in such a way to
func (g *Generator) genInterfaces(into io.Writer, conf *config.DbConfig) error {
	g.log.Infof("\tgenerating DBQueries interface\n")

	var genCtx ifaceGenCtx

	// populate tables
	g.log.Infof("\t\tpopulating tables\n")
	genCtx.Tables = make([]tableIfaceGenCtx, 0, len(conf.Tables))
	for _, tc := range conf.Tables {
		tableInfo, ok := g.metaResolver.TableMeta(tc.Name)
		if !ok {
			return fmt.Errorf("could get schema info about table '%s'", tc.Name)
		}

		genCtx.Tables = append(genCtx.Tables, tableIfaceGenCtx{
			GoName:     tableInfo.Info.GoName,
			PkeyType:   tableInfo.Info.PkeyCol.TypeInfo.Name,
			BoxResults: tableInfo.Config.BoxResults,
		})
	}

	// poplulate queries
	g.log.Infof("\t\tpopulating queries\n")
	genCtx.Queries = make([]meta.QueryMeta, 0, len(conf.Queries))
	for i := range conf.Queries {
		meta, err := g.metaResolver.QueryMeta(&conf.Queries[i], true /* inferArgTypes */)
		if err != nil {
			return err
		}
		genCtx.Queries = append(genCtx.Queries, meta)
	}

	// populate the statement gen ctx
	g.log.Infof("\t\tpopulating statements\n")
	genCtx.Stmts = make([]meta.StmtMeta, 0, len(conf.Stmts))
	for i := range conf.Stmts {
		meta, err := g.metaResolver.StmtMeta(&conf.Stmts[i])
		if err != nil {
			return err
		}
		genCtx.Stmts = append(genCtx.Stmts, meta)
	}

	return dbQueriesTmpl.Execute(into, genCtx)
}

type tableIfaceGenCtx struct {
	GoName     string
	PkeyType   string
	BoxResults bool
}

type ifaceGenCtx struct {
	Tables      []tableIfaceGenCtx
	Queries     []meta.QueryMeta
	StoredFuncs []meta.QueryMeta
	Stmts       []meta.StmtMeta
}

var dbQueriesTmpl *template.Template = template.Must(template.New("db-queries-tmpl").Parse(`

type DBQueries interface {
	//
	// automatic CRUD methods
	//

	{{ range .Tables }}
	// {{ .GoName }} methods
	Get{{ .GoName }}(ctx context.Context, id {{ .PkeyType }}, opts ...pggen.GetOpt) ({{- if .BoxResults }}*{{- end }}{{ .GoName }}, error)
	List{{ .GoName }}(ctx context.Context, ids []{{ .PkeyType }}, opts ...pggen.ListOpt) ([]{{- if .BoxResults }}*{{- end }}{{ .GoName }}, error)
	Insert{{ .GoName }}(ctx context.Context, value {{ if .BoxResults }}*{{ end }}{{ .GoName }}, opts ...pggen.InsertOpt) ({{- if .BoxResults }}*{{- end }}{{ .GoName }}, error)
	BulkInsert{{ .GoName }}(ctx context.Context, values []{{ .GoName }}, opts ...pggen.InsertOpt) ([]{{- if .BoxResults }}*{{- end }}{{ .GoName }}, error)
	Update{{ .GoName }}(ctx context.Context, value {{ if .BoxResults }}*{{ end }}{{ .GoName }}, fieldMask pggen.FieldSet, opts ...pggen.UpdateOpt) ({{- if .BoxResults }}*{{- end }}{{ .GoName }}, error)
	Upsert{{ .GoName }}(ctx context.Context, value {{ if .BoxResults }}*{{ end }}{{ .GoName }}, constraintNames []string, fieldMask pggen.FieldSet, opts ...pggen.UpsertOpt) ({{- if .BoxResults }}*{{- end }}{{ .GoName }}, error)
	BulkUpsert{{ .GoName }}(ctx context.Context, values []{{ .GoName }}, constraintNames []string, fieldMask pggen.FieldSet, opts ...pggen.UpsertOpt) ([]{{- if .BoxResults }}*{{- end }}{{ .GoName }}, error)
	Delete{{ .GoName }}(ctx context.Context, id {{ .PkeyType }}, opts ...pggen.DeleteOpt) error
	BulkDelete{{ .GoName }}(ctx context.Context, ids []{{ .PkeyType }}, opts ...pggen.DeleteOpt) error
	{{ end }}

	//
	// query methods
	//

	{{ range $i, $query := .Queries }}
	{{ if .ConfigData.SingleResult }}
	// {{ .ConfigData.Name }} query
	{{ .ConfigData.Name }}(
		ctx context.Context,
		{{- range .Args }}
		{{- if $query.ConfigData.NullableArguments }}
		{{ .GoName }} {{ .TypeInfo.NullName }},
		{{- else }}
		{{ .GoName }} {{ .TypeInfo.Name }},
		{{- end }}
		{{- end }}
	{{- if (not .MultiReturn) }}
	) ({{ .ReturnTypeName }}, error)
	{{- else }}
	) (*{{ .ReturnTypeName }}, error)
	{{- end }}

	{{ else }}
	// {{ .ConfigData.Name }} query
	{{ .ConfigData.Name }}(
		ctx context.Context,
		{{- range .Args }}
		{{- if $query.ConfigData.NullableArguments }}
		{{ .GoName }} {{ .TypeInfo.NullName }},
		{{- else }}
		{{ .GoName }} {{ .TypeInfo.Name }},
		{{- end }}
		{{- end }}
	) ([]{{- if .ConfigData.BoxResults }}*{{- end }}{{ .ReturnTypeName }}, error)
	{{ .ConfigData.Name }}Query(
		ctx context.Context,
		{{- range .Args }}
		{{ .GoName }} {{ .TypeInfo.Name }},
		{{- end }}
	) (*sql.Rows, error)
	{{ end }}
	{{ end }}

	//
	// stored function methods
	//

	{{ range .StoredFuncs }}
	// {{ .ConfigData.Name }} stored function
	{{ .ConfigData.Name }}(
		ctx context.Context,
		{{- range .Args }}
		{{ .GoName }} {{ .TypeInfo.Name }},
		{{- end }}
	) ([]{{ .ReturnTypeName }}, error)
	{{ .ConfigData.Name }}Query(
		ctx context.Context,
		{{- range .Args }}
		{{ .GoName }} {{ .TypeInfo.Name }},
		{{- end }}
	) (*sql.Rows, error)
	{{ end }}

	//
	// stmt methods
	//

	{{ range .Stmts }}
	// {{ .ConfigData.Name }} stmt
	{{ .ConfigData.Name }}(
		ctx context.Context,
		{{- range .Args}}
		{{ .GoName }} {{ .TypeInfo.Name }},
		{{- end}}
	) (sql.Result, error)
	{{ end }}
}

`))
