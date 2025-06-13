package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"unicode/utf16"
)

type firstLevelIndex struct {
	keyChars []uint16
	offsets  []uint32
}

func decodeFirstLevelIndex(r io.Reader, numEntries uint16) (firstLevelIndex, error) {
	var buf [8]byte
	var index firstLevelIndex

	index.keyChars = make([]uint16, numEntries*4)
	index.offsets = make([]uint32, numEntries)

	for i := range numEntries {
		if _, err := io.ReadFull(r, buf[:8]); err != nil {
			return index, fmt.Errorf("failed to read key char %d: %w", i, err)
		}

		offsetIntoKeyChars := int(i) * 4

		for j := range 4 {
			ch := binary.LittleEndian.Uint16(buf[2*j:])
			index.keyChars[offsetIntoKeyChars+j] = ch
		}
	}

	for i := range numEntries {
		if _, err := io.ReadFull(r, buf[:4]); err != nil {
			return index, fmt.Errorf("failed to read offset %d: %w", i, err)
		}

		index.offsets[i] = binary.LittleEndian.Uint32(buf[:])
	}

	return index, nil
}

func (index firstLevelIndex) offset(s string) (uint32, error) {
	chars := utf16.Encode([]rune(s))

	for i := range index.offsets {
		key := index.keyChars[i*4:][:4]
		if slices.Compare(key, chars) > 0 {
			if i == 0 {
				return 0, fmt.Errorf("%s is before the first entry in the first level index", s)
			}

			return index.offsets[i-1], nil
		}
	}

	// s is after the last key
	return index.offsets[len(index.offsets)-1], nil
}
