package main

import (
	_ "embed"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
)

//go:embed "index.html"
var indexHtmlTemplate string

//go:embed "style.css"
var css string

func main() {
	port := flag.Uint("port", 9454, "the port to serve on")
	flag.Parse()
	path := flag.Arg(0)

	if path == "" {
		slog.Error("missing path to wiki file")
		os.Exit(1)
	}

	addr := fmt.Sprintf("127.0.0.1:%d", *port)
	slog.Info("starting", "addr", addr, "path", path)

	indexTmpl := template.Must(template.New("index").Parse(indexHtmlTemplate))

	wiki, err := OpenWiki(path)
	if err != nil {
		slog.Error("error opening wiki", "path", path, "error", err)
		os.Exit(1)
	}

	http.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		query := r.PostFormValue("query")
		if query == "" {
			if err := indexTmpl.Execute(w, nil); err != nil {
				slog.Error("POST: failed to execute index", "error", err)
			}
			return
		}

		results, err := wiki.query(query)
		if err != nil {
			slog.Error("POST: query failed", "query", query, "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if err := indexTmpl.Execute(w, results); err != nil {
			slog.Error("POST: failed to execute index", "error", err)
		}
	})

	http.HandleFunc("GET /-/{path...}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("path")
		if name == "style.css" {
			w.Header().Set("Content-Type", "text/css")
			if _, err := w.Write([]byte(css)); err != nil {
				slog.Error("GET: Write failed for CSS", "error", err)
			}
			return
		}

		w.WriteHeader(http.StatusNotFound)
	})

	http.HandleFunc("GET /{name...}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if name == "" {
			if err := indexTmpl.Execute(w, nil); err != nil {
				slog.Error("GET: failed to execute index", "error", err)
			}
			return
		}
		if name == "favicon.ico" {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		offsetStr := r.URL.Query().Get("offset")

		var offset int64
		if offsetStr == "" {
			offset, err = wiki.entryOffset(name)
			if err != nil {
				slog.Error("GET: entryOffset failed", "name", name, "error", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		} else {
			offset, err = strconv.ParseInt(offsetStr, 10, 64)
			if err != nil {
				slog.Error("GET: ParseInt failed", "name", name, "offset", offsetStr, "error", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}

		rdr, err := wiki.entryAt(offset)
		if err != nil {
			slog.Error("GET: entryAt failed", "name", name, "offset", offset, "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if _, err = io.Copy(w, rdr); err != nil {
			slog.Error("GET: Copy failed", "name", name, "offset", offset, "error", err)
		}
	})

	slog.Error("exiting", "error", http.ListenAndServe(addr, nil))
}
