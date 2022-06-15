package fuse

import "github.com/yoogottamk/sqlfs/sqlutils"

// TODO: implement for more backends (mysql, pg, etc) and add ability to choose
var backend = sqlutils.SQLiteBackend{}
