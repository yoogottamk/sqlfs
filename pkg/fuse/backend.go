package fuse

import "github.com/yoogottamk/sqlfs/pkg/sqlutils"

// Backend should set from outside. No defaults
var Backend sqlutils.SQLBackend
