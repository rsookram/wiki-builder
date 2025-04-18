// Input: Path of directory to dumped wiki contents
//
// Output files:
//
// Entries
// - number of entries in base-10 as a string, newline
// - newline separated entries (each is a path to the file on disk)
//
// Redirects
// - number of redirects in base-10 as a string, newline
// - newline separated redirects
//   - name to redirect from
//   - tab separator
//   - index into entries from above in base-10 as a string, newline
//
// All strings are encoded in UTF-8
package main

import (
	"bufio"
	"flag"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
	"unicode/utf16"
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
	if dataDir == "" {
		panic("missing required arguments")
	}

	entriesFile, err := os.Create(filepath.Join(dataDir, "stage-0-entries.txt"))
	if err != nil {
		panic(err)
	}
	defer entriesFile.Close()

	redirectsFile, err := os.Create(filepath.Join(dataDir, "stage-0-redirects.txt"))
	if err != nil {
		panic(err)
	}
	defer redirectsFile.Close()

	output := bufio.NewWriterSize(entriesFile, 1024*1024)

	entries, redirects := readData(dataDir)

	writeEntries(output, entries)

	if err := output.Flush(); err != nil {
		panic(err)
	}

	output.Reset(redirectsFile)

	writeRedirects(output, redirects)

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

func writeEntries(output *bufio.Writer, entries []entry) {
	if _, err := output.WriteString(strconv.FormatInt(int64(len(entries)), 10)); err != nil {
		panic(err)
	}
	if _, err := output.WriteRune('\n'); err != nil {
		panic(err)
	}

	for _, e := range entries {
		if _, err := output.WriteString(e.localPath); err != nil {
			panic(err)
		}

		if _, err := output.WriteRune('\n'); err != nil {
			panic(err)
		}
	}
}

func writeRedirects(output *bufio.Writer, redirects []redirect) {
	if _, err := output.WriteString(strconv.FormatInt(int64(len(redirects)), 10)); err != nil {
		panic(err)
	}
	if _, err := output.WriteRune('\n'); err != nil {
		panic(err)
	}

	for _, r := range redirects {
		if _, err := output.WriteString(r.name); err != nil {
			panic(err)
		}
		if _, err := output.WriteRune('\t'); err != nil {
			panic(err)
		}

		if _, err := output.WriteString(strconv.FormatInt(int64(r.entryIdx), 10)); err != nil {
			panic(err)
		}
		if _, err := output.WriteRune('\n'); err != nil {
			panic(err)
		}
	}
}

type entry struct {
	localPath string
}

type exceptionEntry struct {
	localPath string
	name      string
}

type rawRedirect struct {
	name      string
	entryName string
}

// redirect is a resolved version of rawRedirect.
type redirect struct {
	name     string
	entryIdx int
}

func readData(dataDir string) ([]entry, []redirect) {
	dir := filepath.Join(dataDir, "A")

	var entries []entry
	entryToID := make(map[string]int)
	var rawRedirects []rawRedirect
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			panic(err)
		}

		name, _ := strings.CutPrefix(path, dir+"/")

		nameUTF16 := utf16.Encode([]rune(name))
		if len(nameUTF16) > 127 {
			return nil
		}

		// Check for redirect
		fileSize := info.Size()
		if fileSize < 1024 {
			target := getRedirect(path, fileSize)
			originalTarget := target
			if target == ".." {
				target = filepath.Dir(name)
			} else if target == "../.." {
				// This case is extremely rare (one instance in the small version), and
				// this way of handling it seems fine.
				target = filepath.Dir(name)
			}

			if strings.HasPrefix(target, "../") {
				// Example:
				// - name: JAWS/ジョーズ
				// - target: ../ジョーズ
				// - newTarget: ジョーズ
				newTarget := filepath.Join(filepath.Dir(name), target)
				// Sometimes there's an extra "../", so remove it.
				target, _ = strings.CutPrefix(newTarget, "../")
			}

			if strings.Contains(name, "/") && !strings.HasPrefix(originalTarget, "..") {
				target = filepath.Join(filepath.Dir(name), target)
			}

			rawRedirects = append(rawRedirects, rawRedirect{name, target})
			return nil
		}

		entryToID[name] = len(entries)
		entries = append(entries, entry{localPath: path})

		return nil
	})
	if err != nil {
		panic(err)
	}

	exceptionEntries, exceptionRawRedirects := processExceptions(dataDir)
	for _, e := range exceptionEntries {
		entryToID[e.name] = len(entries)
		entries = append(entries, entry{e.localPath})
	}
	for _, r := range exceptionRawRedirects {
		rawRedirects = append(rawRedirects, r)
	}

	redirects := createRedirects(rawRedirects, entryToID)

	return entries, redirects
}

func processExceptions(dataDir string) ([]exceptionEntry, []rawRedirect) {
	dir := filepath.Join(dataDir, "_exceptions")

	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}

	var entries []exceptionEntry
	var rawRedirects []rawRedirect

	for _, dirEntry := range dirEntries {
		info, err := dirEntry.Info()
		if err != nil {
			panic(err)
		}

		fileName := dirEntry.Name()
		if strings.HasPrefix(fileName, "X") {
			continue
		}

		localPath := filepath.Join(dir, fileName)
		path := strings.Replace(fileName, "%2f", "/", -1)

		entryName, _ := strings.CutPrefix(path, "A/")

		nameUTF16 := utf16.Encode([]rune(entryName))
		if len(nameUTF16) > 127 {
			continue
		}

		// Check for redirect
		fileSize := info.Size()
		if fileSize < 1024 {
			target := getRedirect(localPath, fileSize)
			originalTarget := target
			if target == ".." {
				target = filepath.Dir(entryName)
			} else if target == "../.." {
				target = filepath.Dir(entryName)
			} else if target == "/" {
				// I've only seen one case of this in the small version.
				target = entryName + "/"
			}

			if strings.HasPrefix(target, "../") {
				// Example:
				// - name: JAWS/ジョーズ
				// - target: ../ジョーズ
				// - newTarget: ジョーズ
				newTarget := filepath.Join(filepath.Dir(entryName), target)
				target, _ = strings.CutPrefix(newTarget, "../")
			}

			if strings.Contains(entryName, "/") && !strings.HasPrefix(originalTarget, "..") {
				target = filepath.Join(filepath.Dir(entryName), target)
			}

			target, _ = strings.CutPrefix(target, "/")

			rawRedirects = append(rawRedirects, rawRedirect{entryName, target})
			continue
		}

		entries = append(
			entries,
			exceptionEntry{localPath: localPath, name: entryName},
		)
	}

	return entries, rawRedirects
}

func createRedirects(rawRedirects []rawRedirect, entryToID map[string]int) []redirect {
	redirects := make([]redirect, 0, len(rawRedirects))
	for _, r := range rawRedirects {
		if t, found := entryToID[r.entryName]; found {
			redirects = append(redirects, redirect{name: r.name, entryIdx: t})
		}
	}

	return redirects
}

var redirectBuf = make([]byte, 1024)

func getRedirect(path string, size int64) string {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}

	_, err = io.ReadAtLeast(f, redirectBuf, int(size))
	if err != nil {
		panic(err)
	}

	content := string(redirectBuf[:size])
	startStr := `http-equiv="refresh" content="0;url=`
	idx := strings.Index(content, startStr)
	if idx < 0 {
		panic("couldn't find startStr")
	}

	content = content[idx+len(startStr):]

	end := strings.IndexByte(content, '"')
	if end < 0 {
		panic("couldn't find end quote")
	}

	unescaped, err := url.PathUnescape(content[:end])
	if err != nil {
		panic(err)
	}

	return unescaped
}
