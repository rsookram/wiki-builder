package storage

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"unicode/utf16"
)

type Redirect struct {
	NameUTF16 []uint16
	EntryIdx  int
}

func ReadRedirects(rdr *bufio.Reader, dataDir string) []Redirect {
	f, err := os.Open(filepath.Join(dataDir, "stage-0-redirects.txt"))
	if err != nil {
		panic(fmt.Sprintf("Error reading redirects from index-fs: %s", err))
	}
	defer f.Close()

	rdr.Reset(f)

	numRedirects := readInt(rdr)
	redirects := make([]Redirect, numRedirects)

	for i := range numRedirects {
		name := readString(rdr, '\t')
		index := readInt(rdr)

		redirects[i] = Redirect{utf16.Encode([]rune(name)), index}
	}

	return redirects
}
