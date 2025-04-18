// File format:
//
// Note: All multi-byte values are in little endian
//
// Entries
// each entry is zlib compressed, prefixed with its compressed length (u24)
// and packed
//
// Second level index:
// - The key in each row is compressed using incremental encoding
// - The row starts with a common prefix length (u8)
// - Then a length-prefixed (u8) string in UTF-16LE followed by an
// offset (u40) to an entry relative to the start of the entries
// u32 for length of second level index in bytes (including this length)
//
// First level index:
// - packed strings: 8 B string, followed by 8 B string...
// - then packed offsets: u32, u32, ... (used to read the part of the second
// level where the names start with the associated prefix)
// - the offset is relative to the start of the second level index (after its
// length)
// u16 for length of first level index in bytes (including this length)
// - the number of entries will be inferred by the size of the index:
// (size - 2) / 12. Strings are UTF-16LE.
//
// Can do a scan (or binary search) on the packed strings to find the index of
// the correct offset for a query.
// Then get that offset by index.
package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"
	"slices"
	"strings"
	"unicode/utf16"

	"github.com/rsookram/wiki-builder/internal/storage"
)

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
	outputPath := flag.Arg(1)
	if dataDir == "" || outputPath == "" {
		panic("missing required arguments")
	}

	if !strings.HasSuffix(dataDir, string(os.PathSeparator)) {
		dataDir = dataDir + string(os.PathSeparator)
	}

	outputFile, err := os.Create(outputPath)
	if err != nil {
		panic(err)
	}
	defer outputFile.Close()

	compressedEntriesFile, err := os.Open(filepath.Join(dataDir, "stage-1-entries.dat"))
	if err != nil {
		panic(fmt.Sprintf("Error reading entries from compress-entries: %s", err))
	}
	defer compressedEntriesFile.Close()

	output := bufio.NewWriterSize(outputFile, 1024*1024)

	if _, err := io.Copy(output, compressedEntriesFile); err != nil {
		panic(err)
	}

	rdr := bufio.NewReaderSize(nil, 1024*1024)
	redirects := storage.ReadRedirects(rdr, dataDir)

	writtenEntries := storage.ReadEntryMetadata(rdr, dataDir)

	secondLevelRows := createSecondLevelIndex(writtenEntries, redirects)
	log.Println("Finished creating second level index")

	firstLevelIndex := writeSecondLevel(output, secondLevelRows)
	log.Println("Finished creating first level index")

	writeFirstLevel(output, firstLevelIndex)
	log.Println("Finished writing indexes")

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

type firstLevelIndex struct {
	keys    []firstLevelIndexKey
	offsets []uint32
}

func (i *firstLevelIndex) Append(key firstLevelIndexKey, offset uint32) {
	i.keys = append(i.keys, key)
	i.offsets = append(i.offsets, offset)
}

func writeFirstLevel(w io.Writer, index firstLevelIndex) {
	totalSize := uint16((len(index.keys) * (8 + 4)) + 2) // +2 to include the size of `totalSize`

	bb := make([]byte, 0, totalSize)
	for _, k := range index.keys {
		bb = k.Append(bb)
	}
	for _, offset := range index.offsets {
		bb = binary.LittleEndian.AppendUint32(bb, offset)
	}

	bb = binary.LittleEndian.AppendUint16(bb, totalSize)
	if _, err := w.Write(bb); err != nil {
		panic(err)
	}
}

type secondLevelIndexRow struct {
	nameUTF16 []uint16
	offset    uint64
}

func newSecondLevelIndexRow(name []uint16, offset uint64) secondLevelIndexRow {
	return secondLevelIndexRow{
		nameUTF16: name,
		offset:    offset,
	}
}

func createSecondLevelIndex(entries storage.EntryMetadata, redirects []storage.Redirect) []secondLevelIndexRow {
	rows := make([]secondLevelIndexRow, 0, entries.Len()+len(redirects))

	for i := range entries.Len() {
		offset := entries.StartOffset(i)

		rows = append(rows, newSecondLevelIndexRow(entries.Name(i), offset))
	}

	for _, r := range redirects {
		i := r.EntryIdx

		offset := entries.StartOffset(i)

		rows = append(rows, newSecondLevelIndexRow(r.NameUTF16, offset))
	}

	slices.SortFunc(rows, func(a, b secondLevelIndexRow) int {
		return slices.Compare(a.nameUTF16, b.nameUTF16)
	})

	return rows
}

func writeSecondLevel(w io.Writer, rows []secondLevelIndexRow) firstLevelIndex {
	totalSize := uint32(0)

	var firstLevelIndex firstLevelIndex
	prevFirstLevelKey := newFirstLevelIndexKey(rows[0].nameUTF16)
	firstLevelIndex.Append(prevFirstLevelKey, 0)
	countForPrevKey := 0

	var bb []byte
	var prevKey []uint16
	for _, r := range rows {
		currFirstLevelIndexKey := newFirstLevelIndexKey(r.nameUTF16)
		shouldCompress := true
		if countForPrevKey >= 1024 && currFirstLevelIndexKey != prevFirstLevelKey {
			// We need to be able to jump to this key, so it can't be compressed.
			shouldCompress = false
			firstLevelIndex.Append(currFirstLevelIndexKey, totalSize)
			countForPrevKey = 0
		}
		prevFirstLevelKey = currFirstLevelIndexKey
		countForPrevKey++

		numChars := len(r.nameUTF16)
		if numChars > 127 {
			panic(fmt.Sprintf(
				"found a key that is too long: len=%d, %v",
				numChars,
				string(utf16.Decode(r.nameUTF16)),
			))
		}

		// Using incremental encoding / front compression for the key:
		// https://en.wikipedia.org/wiki/Incremental_encoding

		// Write common prefix length (how many chars to reuse from previous key)
		commonLen := commonPrefixLen(prevKey, r.nameUTF16)
		if !shouldCompress {
			commonLen = 0
		}
		bb = append(bb, commonLen)
		totalSize += 1

		// Write length (in characters) prefix
		remainingLen := byte(numChars) - commonLen
		bb = append(bb, remainingLen)
		totalSize += 1

		// Write new part of key
		for _, ch := range r.nameUTF16[commonLen:] {
			bb = binary.LittleEndian.AppendUint16(bb, ch)
		}
		totalSize += uint32(remainingLen) * 2

		prevKey = r.nameUTF16

		// Write offset
		bb = appendOffset(bb, r.offset)
		totalSize += 5

		if _, err := w.Write(bb); err != nil {
			panic(err)
		}
		bb = bb[:0]
	}

	totalSize += 4 // Include the size of `totalSize`
	bb = binary.LittleEndian.AppendUint32(bb, totalSize)
	if _, err := w.Write(bb); err != nil {
		panic(err)
	}

	return firstLevelIndex
}

func commonPrefixLen(lhs, rhs []uint16) byte {
	maxPossible := byte(min(len(lhs), len(rhs)))
	for i := range maxPossible {
		if lhs[i] != rhs[i] {
			return i
		}
	}

	return maxPossible
}

func appendOffset(bb []byte, v uint64) []byte {
	return append(bb,
		byte(v),
		byte(v>>8),
		byte(v>>16),
		byte(v>>24),
		byte(v>>32),
	)
}

type firstLevelIndexKey struct {
	ch0 uint16
	ch1 uint16
	ch2 uint16
	ch3 uint16
}

func newFirstLevelIndexKey(chars []uint16) firstLevelIndexKey {
	var p firstLevelIndexKey

	p.ch0 = chars[0]
	if len(chars) > 1 {
		p.ch1 = chars[1]
	}
	if len(chars) > 2 {
		p.ch2 = chars[2]
	}
	if len(chars) > 3 {
		p.ch3 = chars[3]
	}

	return p
}

func (p firstLevelIndexKey) Append(bb []byte) []byte {
	bb = binary.LittleEndian.AppendUint16(bb, p.ch0)
	bb = binary.LittleEndian.AppendUint16(bb, p.ch1)
	bb = binary.LittleEndian.AppendUint16(bb, p.ch2)
	bb = binary.LittleEndian.AppendUint16(bb, p.ch3)

	return bb
}

func (p firstLevelIndexKey) String() string {
	chars := []uint16{p.ch0, p.ch1, p.ch2, p.ch3}

	length := 1
	if p.ch1 != 0 {
		length++
	}
	if p.ch2 != 0 {
		length++
	}
	if p.ch3 != 0 {
		length++
	}

	return string(utf16.Decode(chars[:length]))
}
