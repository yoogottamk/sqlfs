pragma foreign_keys=on;

create table if not exists metadata (
    inode  integer primary key autoincrement,

    uid    integer not null,
    gid    integer not null,

    mode   integer not null,
    type   integer not null,

    ctime  integer not null,
    atime  integer not null,
    mtime  integer not null,

    name   text    not null,
    size   integer not null default 0
);

create table if not exists filedata (
    inode  integer unique not null references metadata(inode) on delete cascade,
    data   blob    default null
);

create table if not exists parent (
    pinode integer not null,
    inode  integer not null,

    unique (pinode, inode)
);
