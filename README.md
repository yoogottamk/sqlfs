# `sqlfs`
A fuse filesystem that stores data on an SQL dbs.

## How to run
By default, this creates a sqlite database named `fs.sql` in the current directory. To know more or use other sql dbs, run `sqlfs`.

```sh
go install github.com/yoogottamk/sqlfs@latest
mkdir mnt

# initialize the db
sqlfs init

# mounts the fuse fs
sqlfs mount
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
- divide file contents into blocks
- symlinks
- figure out logging
- `.`, `..` in directory listing (`cd ..` works but `.`, `..` aren't shown in `ls -a`)
- more extensive tests [have basic e2e file and dir operations being tested right now, need to verify stuff at sql tables level]

^listed in the order of priority

## Why?
Why not? 🙃

## Other SQL DBs
Some documentation is provided in the long description of `sqlfs` itself. This section aims to document how I tested my implementation.

### MySQL
```sh
docker run --rm -it --name sqlfs-mariadb \
    -e MARIADB_USER=user \
    -e MARIADB_PASSWORD=password \
    -e MARIADB_DATABASE=sqlfs \
    -e MARIADB_RANDOM_ROOT_PASSWORD=yes \
    -p 3306:3306 \
    mariadb:latest

mkdir mnt
sqlfs init -u mysql://user:password@/sqlfs
sqlfs mount -u mysql://user:password@/sqlfs mnt
```

### Postgres
```sh
docker run --rm -it --name sqlfs-postgres \
    -e POSTGRES_USER=user \
    -e POSTGRES_PASSWORD=password \
    -e POSTGRES_DB=sqlfs \
    -p 5432:5432 \
    postgres:latest

mkdir mnt
sqlfs init -u postgres://user:password@localhost:5432/sqlfs
sqlfs mount -u postgres://user:password@localhost:5432/sqlfs mnt
```

## Note
I'm new with golang and fuse and this is my first big-ish project in this language. The code might have bugs and definitely has performance issues (e.g. [loading the entire file into memory during read](https://github.com/yoogottamk/sqlfs/blob/51f11243ba9bc02af95bf92438852385d262325f/sqlutils/common.go#L161-L163)). There was one time in early development when I called [`InvalidateEntry`](https://pkg.go.dev/bazil.org/fuse@v0.0.0-20200524192727-fb710f7dfd05#Conn.InvalidateEntry) with some arguments and had to reboot my laptop in order to get it to work again (no such inconvenience was experienced after that).

Don't use it for critical applications.

# References
- https://pkg.go.dev/bazil.org/fuse/fs
- [bazil/zipfs](https://github.com/bazil/zipfs)
- [Writing a file system in Go](https://bazil.org/talks/2013-06-10-la-gophers/#1)
