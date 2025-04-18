package storage

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"unicode/utf16"
)

type EntryMetadata struct {
	namesUTF16 [][]uint16
	endOffsets []uint64
}

func (em EntryMetadata) Name(i int) []uint16 {
	return em.namesUTF16[i]
}

func (em EntryMetadata) StartOffset(i int) uint64 {
	if i == 0 {
		return 0
	}

	return em.endOffsets[i-1]
}

func (em EntryMetadata) Len() int {
	return len(em.namesUTF16)
}

func ReadEntryMetadata(rdr *bufio.Reader, dataDir string) EntryMetadata {
	f, err := os.Open(filepath.Join(dataDir, "stage-1-entry-meta.txt"))
	if err != nil {
		panic(fmt.Sprintf("Error reading entry metadata from compress-entries %s", err))
	}
	defer f.Close()

	rdr.Reset(f)

	numEntries := readInt(rdr)
	names := make([][]uint16, numEntries)
	endOffsets := make([]uint64, numEntries)

	for i := range numEntries {
		name := readString(rdr, '\n')

		names[i] = utf16.Encode([]rune(name))
	}

	for i := range numEntries {
		offset := readUint64(rdr)
		endOffsets[i] = offset
	}

	return EntryMetadata{names, endOffsets}
}
