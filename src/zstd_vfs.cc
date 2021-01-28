/*
** SQLite loadable extension providing zstd VFS
*/
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1
#include "web_vfs.h"
#include "zstd_vfs.h"

/*************************************************************************************************/

/*
** This routine is called when the extension is loaded.
** Register the new VFS.
*/
extern "C" int sqlite3_zstdvfs_init(sqlite3 *db, char **pzErrMsg,
                                    const sqlite3_api_routines *pApi) {
    int rc = SQLITE_OK;
    SQLITE_EXTENSION_INIT2(pApi);
    rc = (new WebVFS::VFS())->Register("web");
    if (rc != SQLITE_OK) {
        return rc;
    }
    rc = (new ZstdVFS())->Register("zstd");
    if (rc == SQLITE_OK) {
        rc = SQLITE_OK_LOAD_PERMANENTLY;
    }
    return rc;
}
