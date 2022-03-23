//go:build generate
// +build generate

package main

import (
	"bytes"
	"go/format"
	"math"
	"os"
	"reflect"
	"text/template"

	ds "github.com/ipfs/go-datastore"
)

var tmpl = template.Must(template.New("").Parse(`// Code generated by go generate; DO NOT EDIT.

package scoped

import (
	ds "github.com/ipfs/go-datastore"
)

{{ range $idx, $features := .StructFeatures -}}
type ds{{ $idx }} struct {
	ds.Datastore
	{{- range $feat := $features }}
	ds.{{ $feat.IFace }}
	{{- end }}
}
func (d *ds{{ $idx }}) Children() []ds.Datastore {
	return []ds.Datastore{d.Datastore}
}
{{ end }}
var ctors = map[uint]func(ds.Datastore) ds.Datastore{
	{{- range $idx, $features := .StructFeatures }}
	{{ $idx }}: func(dstore ds.Datastore) ds.Datastore {
		return &ds{{ $idx }}{
			Datastore: dstore,
			{{- range $feat := $features }}
			{{ $feat.IFace }}: dstore.(ds.{{ $feat.DatastoreIFace }}),
			{{- end }}
		}
	},
	{{- end }}
}
`))

func main() {
	type feat struct {
		IFace          string
		DatastoreIFace string
	}

	type templateData struct {
		StructFeatures [][]feat
		Features       []feat
	}

	tmplData := templateData{}
	features := ds.Features()

	for _, f := range features {
		tmplData.Features = append(tmplData.Features, feat{
			IFace:          reflect.TypeOf(f.Interface).Elem().Name(),
			DatastoreIFace: reflect.TypeOf(f.DatastoreInterface).Elem().Name(),
		})
	}

	numStructs := int(math.Pow(2, float64(len(features))))
	tmplData.StructFeatures = make([][]feat, numStructs)
	for i := 0; i < numStructs; i++ {
		for bit := 0; bit < len(features); bit++ {
			if ((i >> bit) & 1) == 1 {
				tmplData.StructFeatures[i] = append(tmplData.StructFeatures[i], tmplData.Features[bit])
			}
		}
	}

	buf := bytes.Buffer{}
	err := tmpl.Execute(&buf, tmplData)
	if err != nil {
		panic(err)
	}

	b, err := format.Source(buf.Bytes())
	if err != nil {
		panic(err)
	}

	f, err := os.Create("impls.gen.go")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	_, err = f.Write(b)
	if err != nil {
		panic(err)
	}
}
