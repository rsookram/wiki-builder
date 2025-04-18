// Input: Path of directory to dumped wiki contents
//
// Output files:
//
// Entries
// - each entry is zlib compressed, prefixed with its compressed length (u24)
// and packed
//
// Entry metadata
// - number of entries as a string, newline
// - each entry name, newline separated
// - the end offset of each entry as a string, newline separated
//
// All strings are encoded in UTF-8. All numbers are in base-10.
package main

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"

	"github.com/rsookram/wiki-builder/internal/storage"
)

type writtenEntry struct {
	name      string
	endOffset uint64
}

var bufPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 64*1024))
	},
}

var tmpBufPool = sync.Pool{
	New: func() any {
		return make([]byte, 32*1024)
	},
}

var zlibPool = sync.Pool{
	New: func() any {
		return zlib.NewWriter(nil)
	},
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write memory profile to this file")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	dataDir := flag.Arg(0)
	if dataDir == "" {
		panic("missing required arguments")
	}

	if !strings.HasSuffix(dataDir, string(os.PathSeparator)) {
		dataDir = dataDir + string(os.PathSeparator)
	}

	entriesFile, err := os.Create(filepath.Join(dataDir, "stage-1-entries.dat"))
	if err != nil {
		panic(err)
	}
	defer entriesFile.Close()

	output := bufio.NewWriterSize(entriesFile, 1024*1024)

	rdr := bufio.NewReaderSize(nil, 1024*1024)
	entries := storage.ReadEntries(rdr, dataDir)

	writtenEntries := writeEntries(output, entries)

	if err := output.Flush(); err != nil {
		panic(err)
	}

	f, err := os.Create(filepath.Join(dataDir, "stage-1-entry-meta.txt"))
	if err != nil {
		panic(err)
	}
	defer f.Close()

	output.Reset(f)

	// TODO: Eventually just write start offsets in the original order of the
	// entries. And then entries can be written compressed out of order
	writeEntryMeta(output, writtenEntries)

	if err := output.Flush(); err != nil {
		panic(err)
	}

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			panic(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
		return
	}
}

func writeEntries(w io.Writer, entries []storage.Entry) []writtenEntry {
	writtenEntries := make([]writtenEntry, len(entries))

	results := make([]chan *bytes.Buffer, len(entries))
	for i := range results {
		results[i] = make(chan *bytes.Buffer, 1)
	}

	// Limit parallelism
	tokens := make(chan struct{}, runtime.NumCPU())
	for range runtime.NumCPU() {
		tokens <- struct{}{}
	}

	go func() {
		for i, e := range entries {
			<-tokens

			go func(idx int, path string) {
				results[idx] <- compress(path)
			}(i, e.LocalPath)
		}
	}()

	tmp := make([]byte, 4)
	endOffset := uint64(0)
	for i, e := range entries {
		buf := <-results[i]
		tokens <- struct{}{}

		sizeBytes := uint32(buf.Len())
		endOffset += uint64(sizeBytes) + 3 // 3 for length prefix

		if sizeBytes > 1<<24 {
			panic(fmt.Sprintf("entry is too big, size=%d", sizeBytes))
		}

		// Write length prefix
		binary.LittleEndian.PutUint32(tmp, sizeBytes)
		if _, err := w.Write(tmp[:3]); err != nil {
			panic(err)
		}

		// Write compressed data
		if _, err := w.Write(buf.Bytes()); err != nil {
			panic(err)
		}

		bufPool.Put(buf)

		writtenEntries[i] = writtenEntry{e.Name(), endOffset}

		if i%10000 == 0 {
			log.Println(i+1, "/", len(entries))
		}
	}

	log.Println(len(entries), "/", len(entries))

	return writtenEntries
}

func compress(path string) *bytes.Buffer {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	tmp := tmpBufPool.Get().([]byte)
	zw := zlibPool.Get().(*zlib.Writer)
	zw.Reset(buf)

	f, err := os.Open(path)
	if err != nil {
		panic(fmt.Sprintf("failed to open %s: %s", path, err))
	}

	if _, err = io.CopyBuffer(zw, f, tmp); err != nil {
		panic(err)
	}

	if err = zw.Close(); err != nil {
		panic(err)
	}

	zlibPool.Put(zw)
	tmpBufPool.Put(tmp)
	return buf
}

func writeEntryMeta(output *bufio.Writer, entries []writtenEntry) {
	if _, err := output.WriteString(strconv.FormatInt(int64(len(entries)), 10)); err != nil {
		panic(err)
	}
	if _, err := output.WriteRune('\n'); err != nil {
		panic(err)
	}

	for _, e := range entries {
		if _, err := output.WriteString(e.name); err != nil {
			panic(err)
		}

		if _, err := output.WriteRune('\n'); err != nil {
			panic(err)
		}
	}

	for _, e := range entries {
		if _, err := output.WriteString(strconv.FormatInt(int64(e.endOffset), 10)); err != nil {
			panic(err)
		}

		if _, err := output.WriteRune('\n'); err != nil {
			panic(err)
		}
	}
}
