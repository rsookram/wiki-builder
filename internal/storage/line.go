package storage

import (
	"bufio"
	"strconv"
)

func readInt(r *bufio.Reader) int {
	s := readString(r, '\n')

	num, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}

	return num
}

func readUint64(r *bufio.Reader) uint64 {
	s := readString(r, '\n')

	num, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		panic(err)
	}

	return num
}

func readString(r *bufio.Reader, delim byte) string {
	s, err := r.ReadString(delim)
	if err != nil {
		panic(err)
	}

	return s[:len(s)-1]
}
