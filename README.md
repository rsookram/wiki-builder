# wiki-builder

`wiki-builder` is a collection of command-line program which convert the
contents of a dump of Wikipedia into a custom file format. Intended for use
with the Japanese Wikipedia, but it should work for other languages too.
[`jwiki`](https://github.com/rsookram/jwiki) is an Android app that can use the
output file format.

## Building

Build the binaries by running the following command in the root of this repo:

```shell
go build -o . . ./cmd/...
```

## Usage

Download a `.zim` file for the Japanese Wikipedia from
[here](https://library.kiwix.org/).

Dump the files using [`zimdump`](https://github.com/openzim/zim-tools). For
example:

```shell
zimdump dump --dir=dump path/to/wikipedia.zim
```

(`--redirect` isn't used since it fails for some files)

Then run the commands in this order:

```shell
./index-fs dump/
./compress-entries dump/
./wiki-builder dump/ wikipedia.wiki
```

The final output file will be at `wikipedia.wiki`.

## Known Limitations

- images aren't supported
