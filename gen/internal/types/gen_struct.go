package types

import (
	"strings"
	"text/template"
)

// file: gen_struct.go
// This file exposes an interface for generating a struct with a Scan routine.
// It is shared between the code for generating a query return value and the
// code for generating table code.

// genCtx should be a meta.TableGenCtx. We ask for an interface{} param to avoid a cyclic
// dependency. Using an interface{} is fine because we are just going to pass it into the
// template evaluator anyway.
func (r *Resolver) EmitStructType(typeName string, genCtx interface{}) error {
	var typeBody strings.Builder
	err := structTypeTmpl.Execute(&typeBody, genCtx)
	if err != nil {
		return err
	}

	var typeSig strings.Builder
	err = structTypeSigTmpl.Execute(&typeSig, genCtx)
	if err != nil {
		return err
	}

	err = r.EmitType(typeName, typeSig.String(), typeBody.String())
	if err != nil {
		return err
	}
	return nil
}

var structTypeSigTmpl *template.Template = template.Must(template.New("table-type-field-sig-tmpl").Parse(`
{{- range .Meta.Info.Cols }}
{{- if .Nullable }}
{{ .GoName }} {{ .TypeInfo.NullName }}
{{- else }}
{{ .GoName }} {{ .TypeInfo.Name }}
{{- end }}
{{- end }}
`))

var structTypeTmpl *template.Template = template.Must(template.New("struct-type-tmpl").Parse(`
type {{ .GoName }} struct {
	{{- range .Meta.Info.Cols }}
	{{- if .Nullable }}
	{{ .GoName }} {{ .TypeInfo.NullName }}
	{{- else }}
	{{ .GoName }} {{ .TypeInfo.Name }}
	{{- end }} ` +
	"`" + `{{ .Tags }}` + "`" + `
	{{- end }}
	{{- range .Meta.AllIncomingReferences }}
	{{- if .OneToOne }}
	{{ .GoPointsFromFieldName }} *{{ .PointsFrom.Info.GoName }} ` +
	"`" + `gorm:"foreignKey:{{ .PointsFromField.GoName }}"` + "`" + `
	{{- else }}
	{{ .GoPointsFromFieldName }} []*{{ .PointsFrom.Info.GoName }} ` +
	"`" + `gorm:"foreignKey:{{ .PointsFromField.GoName }}"` + "`" + `
	{{- end }}
	{{- end }}
	{{- range .Meta.AllOutgoingReferences }}
	{{- /* All outgoing references are 1-1, so we don't check the .OneToOne flag */}}
	{{ .GoPointsToFieldName }} *{{ .PointsTo.Info.GoName }}
	{{- end}}
}
func (r *{{ .GoName }}) Scan(rs *sql.Rows) error {
	// We assume that the columns coming in are ordered in the same way as defined in genTimeColIdxTabFor{{ .GoName }}.
	var nullableTgts nullableScanTgtsFor{{ .GoName }}

	scanTgts := make([]interface{}, len(genTimeColIdxTabFor{{ .GoName }}))
	for _, idx := range genTimeColIdxTabFor{{ .GoName }} {
		scanTgts[idx] = scannerTabFor{{ .GoName }}[idx](r, &nullableTgts)
	}

	err := rs.Scan(scanTgts...)
	if err != nil {
		return err
	}

	{{- range .Meta.Info.Cols }}
	{{- if .Nullable }}
	r.{{ .GoName }} = {{ call .TypeInfo.NullConvertFunc (printf "nullableTgts.scan%s" .GoName) }}
	{{- else if (eq .TypeInfo.Name "time.Time") }}
	r.{{ .GoName }} = {{ printf "nullableTgts.scan%s" .GoName }}.Time
	{{- end }}
	{{- end }}

	return nil
}

type nullableScanTgtsFor{{ .GoName }} struct {
	{{- range .Meta.Info.Cols }}
	{{- if (or (or .Nullable (eq .TypeInfo.Name "time.Time")) (eq .TypeInfo.Name "time.Duration")) }}
	scan{{ .GoName }} {{ .TypeInfo.ScanNullName }}
	{{- end }}
	{{- end }}
}

// a table mapping codegen-time col indicies to functions returning a scanner for the
// field that was at that column index at codegen-time.
var scannerTabFor{{ .GoName }} = [...]func(*{{ .GoName }}, *nullableScanTgtsFor{{ .GoName }}) interface{} {
	{{- range .Meta.Info.Cols }}
	func (
		r *{{ $.GoName }},
		nullableTgts *nullableScanTgtsFor{{ $.GoName }},
	) interface{} {
		{{- if (or (or .Nullable (eq .TypeInfo.Name "time.Time")) (eq .TypeInfo.Name "time.Duration")) }}
		return {{ call .TypeInfo.NullSqlReceiver (printf "nullableTgts.scan%s" .GoName) }}
		{{- else }}
		return {{ call .TypeInfo.SqlReceiver (printf "r.%s" .GoName) }}
		{{- end }}
	},
	{{- end }}
}

var genTimeColIdxTabFor{{ .GoName }} map[string]int = map[string]int{
	{{- range $i, $col := .Meta.Info.Cols }}
	` + "`" + `{{ $col.PgName }}` + "`" + `: {{ $i }},
	{{- end }}
}

func QueryAndScan{{ .GoName }}(
	ctx context.Context,
	h pggen.DBHandle,
	query string,
	args ...interface{},
) (ret []{{- if .Meta.Config.BoxResults }}*{{- end }}{{ .GoName }}, err error) {
	rows, err := h.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err == nil {
			err = rows.Close()
			if err != nil {
				ret = nil
			}
		} else {
			rowErr := rows.Close()
			if rowErr != nil {
				err = fmt.Errorf("%s AND %s", err.Error(), rowErr.Error())
			}
		}
	}()

	ret = make([]{{- if .Meta.Config.BoxResults }}*{{- end }}{{ .GoName }}, 0)
	for rows.Next() {
		var value {{ .GoName }}
		err = value.Scan(rows)
		if err != nil {
			return nil, err
		}
		ret = append(ret, {{- if .Meta.Config.BoxResults }}&{{- end }}value)
	}

	return ret, nil
}
`))
