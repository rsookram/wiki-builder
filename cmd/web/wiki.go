package main

import (
	"bufio"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf16"
)

type Wiki struct {
	first                         firstLevelIndex
	secondLevelIndexOffsetFromEnd int64

	file *os.File
	rdr  *bufio.Reader
	buf  []byte
}

func OpenWiki(path string) (Wiki, error) {
	var wiki Wiki

	f, err := os.Open(path)
	if err != nil {
		return wiki, fmt.Errorf("failed to open %s: %w", path, err)
	}
	wiki.file = f

	_, err = f.Seek(-2, io.SeekEnd)
	if err != nil {
		return wiki, fmt.Errorf("failed to seek for first level index size: %w", err)
	}

	buf := make([]byte, 512)
	wiki.buf = buf

	_, err = io.ReadFull(f, buf[:2])
	if err != nil {
		return wiki, fmt.Errorf("failed to read for first level index size: %w", err)
	}

	firstLevelIndexSize := binary.LittleEndian.Uint16(buf)

	firstLevelIndexRowSize := uint16(12)
	numFirstLevelIndexEntries := (firstLevelIndexSize - 2) / firstLevelIndexRowSize

	_, err = f.Seek(-int64(firstLevelIndexSize)-4, io.SeekEnd)
	if err != nil {
		return wiki, fmt.Errorf("failed to seek for second level index: %w", err)
	}

	rdr := bufio.NewReaderSize(f, 16*1024)
	wiki.rdr = rdr

	rdr.Read(buf[:4])

	secondLevelIndexSize := binary.LittleEndian.Uint32(buf)

	firstLevelIndex, err := decodeFirstLevelIndex(rdr, numFirstLevelIndexEntries)
	if err != nil {
		return wiki, fmt.Errorf("failed to decode first level index: %w", err)
	}

	wiki.first = firstLevelIndex
	wiki.secondLevelIndexOffsetFromEnd = int64(firstLevelIndexSize) + int64(secondLevelIndexSize)

	return wiki, nil
}

type SearchResult struct {
	Key         string
	EntryOffset int64
}

func (w *Wiki) query(prefix string) ([]SearchResult, error) {
	if prefix == "" {
		panic("tried to query for an empty string")
	}

	secondLevelIndex, err := w.first.offset(prefix)
	if err != nil {
		return nil, err
	}

	if err := w.seekToSecondLevelIndexOffset(int64(secondLevelIndex)); err != nil {
		return nil, err
	}

	w.rdr.Reset(w.file)

	prefixChars := utf16.Encode([]rune(prefix))

	var headerBuf [2]byte
	var result SearchResult
	for {
		if _, err := io.ReadFull(w.rdr, headerBuf[:]); err != nil {
			return nil, fmt.Errorf("query failed to read second level index entry header: %w", err)
		}

		commonPrefixLen := headerBuf[0]
		numRemainingChars := headerBuf[1]
		numKeyBytes := (int(commonPrefixLen) + int(numRemainingChars)) * 2

		// Read string and offset at once.
		if _, err := io.ReadFull(w.rdr, w.buf[commonPrefixLen*2:][:numRemainingChars*2+5]); err != nil {
			return nil, fmt.Errorf("query failed to read second level index key: %w", err)
		}

		cmp := compareTo(w.buf, prefixChars, numKeyBytes)
		if cmp >= 0 {
			result.Key = w.readString(numKeyBytes)
			result.EntryOffset = int64(entryOffsetToUInt64(w.buf, numKeyBytes))
			break
		}
	}

	limit := 32
	results := make([]SearchResult, 0, limit)
	for i := 0; strings.HasPrefix(result.Key, prefix) && len(results) < limit; i++ {
		results = append(results, result)
		result, err = w.readSecondLevelIndex()
		if err != nil {
			return nil, fmt.Errorf("query failed to read secondLevelIndex: %w", err)
		}
	}

	return results, nil
}

func (w *Wiki) entryOffset(name string) (int64, error) {
	secondLevelIndex, err := w.first.offset(name)
	if err != nil {
		return -1, err
	}

	if err := w.seekToSecondLevelIndexOffset(int64(secondLevelIndex)); err != nil {
		return -1, err
	}

	w.rdr.Reset(w.file)

	nameChars := utf16.Encode([]rune(name))

	var headerBuf [2]byte
	for {
		if _, err := io.ReadFull(w.rdr, headerBuf[:]); err != nil {
			return -1, fmt.Errorf("entryOffset failed to read second level index entry header: %w", err)
		}

		commonPrefixLen := headerBuf[0]
		numRemainingChars := headerBuf[1]
		numKeyBytes := (int(commonPrefixLen) + int(numRemainingChars)) * 2

		// Read string and offset at once.
		if _, err := io.ReadFull(w.rdr, w.buf[commonPrefixLen*2:][:numRemainingChars*2+5]); err != nil {
			return -1, fmt.Errorf("entryOffset failed to read second level index key: %w", err)
		}

		cmp := compareTo(w.buf, nameChars, numKeyBytes)
		if cmp == 0 {
			return int64(entryOffsetToUInt64(w.buf, numKeyBytes)), nil
		} else if cmp > 0 {
			return -1, fmt.Errorf("%s is after the last entry in the second level index", name)
		}
	}
}

func (w *Wiki) entryAt(offset int64) (io.Reader, error) {
	if _, err := w.file.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to entry at %d: %w", offset, err)
	}

	var buf [3]byte
	if _, err := io.ReadFull(w.file, buf[:]); err != nil {
		return nil, fmt.Errorf("failed to read entry length at %d: %w", offset, err)
	}

	compressedSize := entryLength(buf[:])

	// Assume that the data will be read before the next time the file is used
	r, err := zlib.NewReader(io.LimitReader(w.file, int64(compressedSize)))
	if err != nil {
		return nil, fmt.Errorf("zlib NewReader failed for %d; len=%d: %w", offset, compressedSize, err)
	}

	return r, nil
}

func (w *Wiki) readSecondLevelIndex() (SearchResult, error) {
	var headerBuf [2]byte
	if _, err := io.ReadFull(w.rdr, headerBuf[:]); err != nil {
		return SearchResult{}, fmt.Errorf("readSecondLevelIndex failed to read entry header: %w", err)
	}

	commonPrefixLen := headerBuf[0]
	numRemainingChars := headerBuf[1]
	numKeyBytes := (int(commonPrefixLen) + int(numRemainingChars)) * 2

	// Read string and offset at once
	if _, err := io.ReadFull(w.rdr, w.buf[commonPrefixLen*2:][:numRemainingChars*2+5]); err != nil {
		return SearchResult{}, fmt.Errorf("readSecondLevelIndex failed to read entry key: %w", err)
	}

	key := w.readString(numKeyBytes)

	entryOffset := entryOffsetToUInt64(w.buf, numKeyBytes)

	return SearchResult{
		Key:         key,
		EntryOffset: int64(entryOffset),
	}, nil
}

func compareTo(buf []byte, prefixChars []uint16, numBufferBytes int) int {
	for i := range min(numBufferBytes/2, len(prefixChars)) {
		bufCh := binary.LittleEndian.Uint16(buf[i*2:])
		prefixCh := prefixChars[i]

		cmp := int(bufCh) - int(prefixCh)
		if cmp != 0 {
			return cmp
		}
	}

	return numBufferBytes - len(prefixChars)*2
}

func (w *Wiki) readString(numBytes int) string {
	chars := make([]uint16, 0, numBytes/2)

	for i := 0; i < numBytes; i += 2 {
		ch := binary.LittleEndian.Uint16(w.buf[i:])
		chars = append(chars, ch)
	}

	return string(utf16.Decode(chars))
}

func (w *Wiki) seekToSecondLevelIndexOffset(offset int64) error {
	if _, err := w.file.Seek(-w.secondLevelIndexOffsetFromEnd+offset, io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek to %d in second level index: %w", offset, err)
	}

	return nil
}

func entryLength(b []byte) uint32 {
	_ = b[2] // bounds check hint to compiler; see golang.org/issue/14808
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16
}

func entryOffsetToUInt64(b []byte, offset int) uint64 {
	// 5 bytes is plenty for an offset. 2^40 B ~= 1 TB
	_ = b[4] // bounds check hint to compiler; see golang.org/issue/14808
	return uint64(b[offset]) |
		uint64(b[offset+1])<<8 |
		uint64(b[offset+2])<<16 |
		uint64(b[offset+3])<<24 |
		uint64(b[offset+4])<<32
}
