package storage

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf16"
)

// TODO: SOA makes more sense for this
type Entry struct {
	LocalPath      string
	htmlPathOffset int
}

func (e Entry) Name() string {
	htmlPath := e.LocalPath[e.htmlPathOffset:]

	htmlPath, found := strings.CutPrefix(htmlPath, "_exceptions/")
	if found {
		htmlPath = strings.Replace(htmlPath, "%2f", "/", -1)
	}

	return htmlPath[len("A/"):]
}

func (e Entry) NameUTF16() []uint16 {
	return utf16.Encode([]rune(e.Name()))
}

func ReadEntries(rdr *bufio.Reader, dataDir string) []Entry {
	f, err := os.Open(filepath.Join(dataDir, "stage-0-entries.txt"))
	if err != nil {
		panic(fmt.Sprintf("Error reading entries from index-fs: %s", err))
	}
	defer f.Close()

	rdr.Reset(f)

	numEntries := readInt(rdr)
	entries := make([]Entry, numEntries)

	for i := range numEntries {
		localPath := readString(rdr, '\n')
		entries[i] = Entry{localPath, len(dataDir)}
	}

	return entries
}
