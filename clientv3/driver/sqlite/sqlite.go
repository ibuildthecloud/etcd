package sqlite

import (
	"strings"

	"github.com/coreos/etcd/clientv3/driver"
	_ "github.com/mattn/go-sqlite3"
)

var (
	fieldList = "name, value, old_value, old_revision, create_revision, revision, ttl, version, del"
	baseList  = `
SELECT kv.id, kv.name, kv.value, kv.old_value, kv.old_revision, kv.create_revision, kv.revision, kv.ttl, kv.version, kv.del
FROM key_value kv
  INNER JOIN
    (
      SELECT MAX(revision) revision, kvi.name
      FROM key_value kvi
          GROUP BY kvi.name
    ) AS r
    ON r.name = kv.name AND r.revision = kv.revision
WHERE kv.name like ? limit ?
`
	insertSQL = `
INSERT INTO key_value(` + fieldList + `)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
)

func NewSQLite() *driver.Generic {
	return &driver.Generic{
		CleanupSQL:      "DELETE FROM key_value WHERE ttl > 0 AND ttl < ?",
		ListSQL:         strings.Replace(baseList, "%REV%", "", -1),
		ListRevisionSQL: strings.Replace(baseList, "%REV%", "WHERE kvi.revision <= ?", -1),
		InsertSQL:       insertSQL,
		ReplaySQL:       "SELECT id, " + fieldList + " FROM key_value WHERE name like ? and revision <= ?",
		GetRevisionSQL:  "SELECT MAX(revision) FROM key_value",
	}
}
