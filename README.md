# wiki-builder

## Building

Build the binaries by running the following command in the root of this repo:

```shell
go build -o . ./...
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
