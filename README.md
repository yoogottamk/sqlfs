# `sqlfs`
A fuse filesystem that stores data on an SQL dbs.

## How to run
The default flags create a sqlite database, named `fs.sql` in the current directory. To know more, run `./sqlfs`. In particular, look at global flags `-b` and `-d`.

```sh
go build
mkdir mnt

# initialize the db
./sqlfs init

# mounts the fuse fs
./sqlfs mount
# keep this running^

# open a new terminal and
cd mnt

# create, remove files and dirs, read and write files, ...
# ... after you're done, run:
umount mnt
```

## Operations supported
![demo](./.images/demo.png)

## TODO
- tests
- docs
- divide file contents into blocks
- symlinks
- `.`, `..` in directory listing (`cd ..` works but `.`, `..` aren't shown in `ls -a`)

## Why?
Why not? 🙃

## Note
I'm new with golang and fuse and this is my first big-ish project in this language. The code might have bugs and definitely has performance issues (e.g. [loading the entire file into memory during read](https://github.com/yoogottamk/sqlfs/blob/51f11243ba9bc02af95bf92438852385d262325f/sqlutils/common.go#L161-L163)). There was one time in early development when I called [`InvalidateEntry`](https://pkg.go.dev/bazil.org/fuse@v0.0.0-20200524192727-fb710f7dfd05#Conn.InvalidateEntry) with some arguments and had to reboot my laptop in order to get it to work again (no such inconvenience was experienced after that).

Don't use it for critical applications.

# References
- https://pkg.go.dev/bazil.org/fuse/fs
- [bazil/zipfs](https://github.com/bazil/zipfs)
- [Writing a file system in Go](https://bazil.org/talks/2013-06-10-la-gophers/#1)
