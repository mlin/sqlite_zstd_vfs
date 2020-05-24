/**
 * SQLite VFS that stores the pages of an "inner" main database file in an "outer" database. See:
 *   https://www.sqlite.org/fileformat.html
 *   https://sqlite.org/zipvfs/doc/trunk/www/howitworks.wiki
 *
 * Instead of writing pages to a calculated file offset, upserts them into the outer pages table,
 * from which they can later be queried as well. This implementation stores the pages as-is which
 * is useless, but subclasses can easily layer in compression and/or encryption codecs.
 */
#pragma once

#include "SQLiteCpp/SQLiteCpp.h"
#include "SQLiteVFS.h"
#include <cstdint>
#include <libgen.h>
#include <memory>
#include <string>
#include <vector>

#include <iostream>
#define _EOL std::endl
#ifndef NDEBUG
#define _DBG std::cerr << __FILE__ << ":" << __LINE__ << ": "
#else
#define _DBG false && std::cerr
#endif

namespace SQLiteNested {
// Implements I/O methods for the "inner" main database file.
// KEY ASSUMPTIONS:
//   1. SQLite only calls the VFS xWrite() with one or more whole pages, and its first xWrite() to
//      a new db is exactly one whole page.
//   2. xSync(), xTruncate(), and xClose() are only called at moments when the inner database will
//      be consistent once previously written pages are all flushed. That is, SQLite would never
//      explicitly request to sync an inconsistent state of the main db file in the middle of a
//      transaction.
class InnerDatabaseFile : public SQLiteVFS::File {
  protected:
    std::unique_ptr<SQLite::Database> outer_db_;
    std::string inner_db_tablename_prefix_; // can be overridden in VFS, not here
    bool read_only_;
    size_t page_size_ = 0, page_count_ = 0, pages_written_ = 0;
    // cached statements; must be declared after outer_db_ so that they're destructed first
    SQLite::Statement select_pages_, select_page_count_, begin_, commit_;
    std::unique_ptr<SQLite::Statement> insert_page_, update_page_, delete_pages_;

    // RAII helper
    class StatementResetter {
      private:
        SQLite::Statement &stmt_;

      public:
        StatementResetter(SQLite::Statement &stmt) : stmt_(stmt) {}
        ~StatementResetter() { stmt_.tryReset(); }
    };

    // On the first Write() we open a transaction with the outer db as a buffer for this and
    // subsequent writes, which we commit upon Sync() or Close(). We never explicitly rollback this
    // transaction, although rollback may occur if the process/host crashes. By Assumption 2 above,
    // inner db should always recover to a consistent state.
    bool txn_ = false;
    void begin() {
        assert(!read_only_);
        if (!txn_) {
            StatementResetter resetter(begin_);
            if (begin_.executeStep()) {
                throw SQLite::Exception("unexpected result from BEGIN", SQLITE_ERROR);
            }
            txn_ = true;
        }
    }
    void commit() {
        if (txn_) {
            StatementResetter resetter(commit_);
            try {
                if (commit_.executeStep()) {
                    throw SQLite::Exception("unexpected result from COMMIT", SQLITE_ERROR);
                }
                txn_ = false;
            } catch (std::exception &exn) {
                _DBG << exn.what() << _EOL;
                page_count_ = 0; // to be redetected
                throw;
            }
        }
    }

    // Update page_size_ (a property of the inner db, immutable once set on first write) based on
    // some inspection of outer db. Leave at zero if the inner db is still new. Default logic takes
    // the size of the data for the first page; subclasses may override.
    virtual size_t DetectPageSize() {
        if (!page_size_) {
            StatementResetter resetter(select_pages_);
            select_pages_.bind(1, 0);
            select_pages_.bind(2, 1);
            if (select_pages_.executeStep()) {
                assert(select_pages_.getColumnCount() >= 2);
                assert(select_pages_.getColumn(1).isBlob());
                assert(select_pages_.getColumnName(1) == std::string("data"));
                int sz = select_pages_.getColumn(1).getBytes();
                if (sz <= 0 || sz > 65536) {
                    throw SQLite::Exception("invalid page size in nested VFS page table",
                                            SQLITE_CORRUPT);
                }
                page_size_ = (size_t)sz;
            }
        }
        return page_size_;
    }

    // Update page_count_ (a mutable property of the inner db) based on some inspection of outer
    // db. Leave it at zero if the inner db is still new.
    //   1. Corruption is likely to occur if page_count_ somehow drifts from the actual outer db
    //      state, e.g. by a concurrent writer. Outer db should have EXCLUSIVE locking mode.
    //   2. Write() may update page_count_ to reflect newly written pages without calling this.
    size_t DetectPageCount() {
        if (!page_count_) {
            StatementResetter resetter(select_page_count_);
            if (!select_page_count_.executeStep()) {
                throw SQLite::Exception("expected result from page count query", SQLITE_ERROR);
            }
            assert(select_page_count_.getColumn(0).isInteger());
            sqlite3_int64 page_count = select_page_count_.getColumn(0);
            assert(page_count >= 0);
            if (page_count > 0) {
                assert(select_page_count_.getColumn(1).isInteger());
                // integrity check, page indices are sequential [0-page_count)
                sqlite3_int64 sum_page_idx = select_page_count_.getColumn(1).getInt64();
                if (sum_page_idx != page_count * (page_count - 1) / 2) {
                    throw SQLite::Exception("missing pages", SQLITE_CORRUPT);
                }
                page_count_ = page_count;
            } else {
                assert(page_count == 0);
                page_count_ = 0;
            }
        }
        return page_count_;
    }

    // Decode one page from the blob stored in the outer db (page_size_ bytes out). Override me!
    virtual void DecodePage(sqlite3_int64 page_idx, const SQLite::Column &data,
                            const SQLite::Column &meta1, const SQLite::Column &meta2, void *dest) {
        if (!data.isBlob() || data.getBytes() != page_size_) {
            throw SQLite::Exception("inconsistent page sizes", SQLITE_CORRUPT);
        }
        memcpy(dest, data.getBlob(), page_size_);
    }

    // Encode one page for storage as a blob in the outer db (page_size_ bytes in). Override me!
    // Bind columns: 1=data, 2=meta1, 3=meta2
    // pagebuf is scratch space that can be resized and overwritten, then use upsert.bindNoCopy()
    virtual void EncodePage(sqlite3_int64 page_idx, const void *src, std::vector<char> &pagebuf,
                            SQLite::Statement &upsert) {
        upsert.bindNoCopy(1, src, page_size_);
    }

#ifndef NDEBUG
    sqlite3_int64 last_page_read_ = -999, sequential_reads_ = 0, non_sequential_reads_ = 0,
                  longest_read_ = 0;
#endif

    int Close() override {
        try {
            commit();
            if (pages_written_) {
                outer_db_->exec("PRAGMA incremental_vacuum");
            }
        } catch (SQLite::Exception &exn) {
            _DBG << exn.what() << _EOL;
            return exn.getErrorCode();
        }
#ifndef NDEBUG
        if (sequential_reads_ || non_sequential_reads_) {
            _DBG << "reads sequential: " << sequential_reads_
                 << " non-sequential: " << non_sequential_reads_ << " longest: " << longest_read_
                 << _EOL;
        }
#endif
        return SQLiteVFS::File::Close();
    }

    int Read(void *zBuf, int iAmt, sqlite3_int64 iOfst) override {
        try {
            assert(iAmt >= 0);
            if (iAmt == 0) {
                return SQLITE_OK;
            }
            if (!DetectPageSize()) { // file is empty
                memset(zBuf, 0, iAmt);
                return iAmt > 0 ? SQLITE_IOERR_SHORT_READ : SQLITE_OK;
            }
            assert(page_size_ <= 65536);
            if (iOfst >= page_size_ && iOfst % page_size_) { // non-aligned read
                return SQLITE_IOERR_READ;
            }

            // calculate page number range
            sqlite3_int64 beg_page = iOfst / page_size_,
                          end_page = 1 + (iOfst + iAmt - 1) / page_size_, prev_page = beg_page - 1;
            int sofar = 0;

            // scan them
            StatementResetter resetter(select_pages_);
            select_pages_.bind(1, beg_page);
            select_pages_.bind(2, end_page);
            while (select_pages_.executeStep()) {
                // check page number sequence
                assert(select_pages_.getColumnCount() >= 4);
                assert(select_pages_.getColumn(0).isInteger());
                assert(select_pages_.getColumnName(0) == std::string("page_idx"));
                assert(select_pages_.getColumn(1).isBlob());
                assert(select_pages_.getColumnName(1) == std::string("data"));
                assert(select_pages_.getColumnName(2) == std::string("meta1"));
                assert(select_pages_.getColumnName(3) == std::string("meta2"));
                sqlite3_int64 page_idx = select_pages_.getColumn(0).getInt64();
                if (page_idx < beg_page || page_idx >= end_page || page_idx != ++prev_page) {
                    throw SQLite::Exception("incomplete page sequence", SQLITE_CORRUPT);
                }
                // decode page & append to zBuf
                SQLite::Column data = select_pages_.getColumn(1);
                int page_ofs = sofar == 0 ? iOfst % page_size_ : 0;
                int desired = std::min(iAmt - sofar, (int)page_size_ - page_ofs);
                if (page_ofs == 0 && desired == page_size_) {
                    // aligned read of a whole page
                    DecodePage(page_idx, data, select_pages_.getColumn(2),
                               select_pages_.getColumn(3), (uint8_t *)zBuf + sofar);
                } else {
                    // non-aligned read (happens when SQLite reads the db header)
                    assert(page_idx == 0);
                    std::vector<char> pagebuf(page_size_, 0);
                    DecodePage(page_idx, data, select_pages_.getColumn(2),
                               select_pages_.getColumn(3), pagebuf.data());
                    memcpy((uint8_t *)zBuf + sofar, pagebuf.data() + page_ofs, desired);
                }
                // update sofar
                sofar += desired;
                assert(sofar <= iAmt);
            }

            // finish up
            if (sofar < iAmt) {
                memset((uint8_t *)zBuf + sofar, 0, iAmt - sofar);
                return SQLITE_IOERR_SHORT_READ;
            }
            assert(sofar == iAmt);
#ifndef NDEBUG
            if (beg_page == last_page_read_ + 1) {
                sequential_reads_++;
            } else {
                non_sequential_reads_++;
            }
            last_page_read_ = end_page - 1;
            longest_read_ = std::max((sqlite3_int64)iAmt, longest_read_);
#endif
            return SQLITE_OK;
        } catch (SQLite::Exception &exn) {
            _DBG << exn.what() << _EOL;
            switch (exn.getErrorCode()) {
            case SQLITE_CORRUPT:
                return SQLITE_CORRUPT;
            }
            return SQLITE_IOERR_READ;
        }
    }

    int Write(const void *zBuf, int iAmt, sqlite3_int64 iOfst) override {
        assert(iAmt >= 0);
        assert(!read_only_);
        try {
            if (!iAmt) {
                return SQLITE_OK;
            }
            if (!DetectPageSize()) { // db is new; assume caller is writing one whole page
                page_size_ = iAmt;
            }
            if (iAmt % page_size_ || iOfst % page_size_) { // non-aligned write
                return SQLITE_IOERR_WRITE;
            }

            if (!insert_page_) {
                insert_page_.reset(new SQLite::Statement(
                    *outer_db_, "INSERT INTO " + inner_db_tablename_prefix_ +
                                    "pages(data,meta1,meta2,page_idx) VALUES(?,?,?,?)"));
            }
            if (!update_page_) {
                update_page_.reset(new SQLite::Statement(
                    *outer_db_, "UPDATE " + inner_db_tablename_prefix_ +
                                    "pages SET data=?, meta1=?, meta2=? WHERE page_idx=?"));
            }

            begin();
            if (iOfst / page_size_ > DetectPageCount()) { // write past current EOF
                AppendBlankPages(iOfst / page_size_ - page_count_);
            }
            assert(iOfst / page_size_ <= page_count_);

            // upsert each provided page
            begin();
            int sofar = 0;
            std::vector<char> pagebuf;
            pagebuf.reserve(page_size_);
            while (sofar < iAmt) {
                assert(!((iOfst + sofar) % page_size_));
                assert(!((iAmt - sofar) % page_size_));
                UpsertPage((iOfst + sofar) / page_size_, (uint8_t *)zBuf + sofar, pagebuf);
                sofar += page_size_;
            }
            assert(sofar == iAmt);
            return SQLITE_OK;
        } catch (std::exception &exn) {
            _DBG << exn.what() << _EOL;
            page_count_ = 0; // to be redetected
            return SQLITE_IOERR_WRITE;
        }
    }

    void AppendBlankPages(int n) {
        try {
            std::string blank_page(page_size_, 0);
            std::vector<char> pagebuf(page_size_);
            for (sqlite3_int64 i = 0; i < n; ++i) {
                insert_page_->clearBindings();
                EncodePage(page_count_, blank_page.data(), pagebuf, *insert_page_);
                insert_page_->bind(4, (sqlite3_int64)page_count_);
                StatementResetter resetter(*insert_page_);
                if (insert_page_->exec() != 1) {
                    throw SQLite::Exception("unexpected result from blank page append",
                                            SQLITE_IOERR_WRITE);
                }
                page_count_++;
            }
        } catch (std::exception &exn) {
            _DBG << exn.what() << _EOL;
            page_count_ = 0; // to be redetected
            throw;
        }
    }

    void UpsertPage(sqlite_int64 page_idx, const void *page, std::vector<char> &pagebuf) {
        assert(page_idx <= page_count_);
        try {
            auto &stmt = page_idx < page_count_ ? update_page_ : insert_page_;
            stmt->clearBindings();
            EncodePage(page_idx, page, pagebuf, *stmt);
            stmt->bind(4, page_idx);
            StatementResetter resetter(*stmt);
            if (stmt->exec() != 1) {
                throw SQLite::Exception("unexpected result from page upsert " +
                                            std::to_string(page_idx),
                                        SQLITE_IOERR_WRITE);
            }
            page_count_ += page_idx < page_count_ ? 0 : 1;
            pages_written_++;
        } catch (std::exception &exn) {
            _DBG << exn.what() << _EOL;
            page_count_ = 0; // to be redetected
            throw;
        }
    }

    int Truncate(sqlite3_int64 size) override {
        assert(!read_only_);
        try {
            if (!DetectPageSize()) {
                return size == 0 ? SQLITE_OK : SQLITE_IOERR_TRUNCATE;
            }
            if (size < 0 || size % page_size_) {
                return SQLITE_IOERR_TRUNCATE;
            }
            sqlite3_int64 new_page_count = size / page_size_;
            if (new_page_count > DetectPageCount()) {
                return SQLITE_IOERR_TRUNCATE;
            }

            if (!delete_pages_) {
                delete_pages_.reset(
                    new SQLite::Statement(*outer_db_, "DELETE FROM " + inner_db_tablename_prefix_ +
                                                          "pages WHERE page_idx >= ?"));
            }
            delete_pages_->bind(1, new_page_count);
            StatementResetter resetter(*delete_pages_);
            delete_pages_->exec();
            commit();
            page_count_ = new_page_count;
            return SQLITE_OK;
        } catch (std::exception &exn) {
            _DBG << exn.what() << _EOL;
            page_count_ = 0; // to be redetected
            return SQLITE_IOERR_TRUNCATE;
        }
    }

    int Sync(int flags) override {
        assert(!read_only_);
        try {
            commit();
        } catch (SQLite::Exception &exn) {
            return exn.getErrorCode();
        }
        return SQLITE_OK;
    }

    int FileSize(sqlite3_int64 *pSize) override {
        try {
            *pSize = DetectPageSize() * DetectPageCount();
            return SQLITE_OK;
        } catch (std::exception &exn) {
            return SQLITE_IOERR;
        }
    }

    int Lock(int eLock) override {
        /*
        No-op: currently we hardcode locking_mode=EXCLUSIVE for the outer db, so there's no need to
        respond to inner db lock calls. This can be relaxed in the future by:
          - when we get Lock(SQLITE_LOCK_SHARED): begin(), re-detect the page count and hold open
            the cursor from that query.
          - when we get Lock(SQLITE_LOCK_EXCLUSIVE): perform some dummy write like
            PRAGMA user_version=(PRAGMA user_version)+1
          - when we get xUnlock(SQLITE_LOCK_SHARED), assert that we recently serviced a Sync()
            (=> committed the outer txn) and no subsequent Read() or Write() ops
          - when we get xUnlock(SQLITE_LOCK_NONE): close the query cursor
          - any use of page_count_ outside of the outer txn should be replaced with detection
        Holding the open cursor is supposed to ensure outer db lock only downgrades from EXCLUSIVE
        to SHARED upon txn commit, rather than releasing entirely. Testing of all this will be
        tricky, so we've punted on it for now.
        */
        return SQLITE_OK;
    }
    int Unlock(int eLock) override { return SQLITE_OK; }
    int CheckReservedLock(int *pResOut) override {
        *pResOut = 0;
        return SQLITE_OK;
    }
    int FileControl(int op, void *pArg) override { return SQLITE_NOTFOUND; }
    int SectorSize() override { return 0; }
    int DeviceCharacteristics() override { return 0; }

    int ShmMap(int iPg, int pgsz, int isWrite, void volatile **pp) override {
        assert(false);
        return SQLITE_IOERR_SHMMAP;
    }
    int ShmLock(int offset, int n, int flags) override {
        assert(false);
        return SQLITE_IOERR_SHMLOCK;
    }
    void ShmBarrier() override {
        assert(false);
        return;
    }
    int ShmUnmap(int deleteFlag) override {
        assert(false);
        return SQLITE_OK;
    }

    int Fetch(sqlite3_int64 iOfst, int iAmt, void **pp) override {
        assert(false);
        *pp = nullptr;
        return SQLITE_IOERR_MMAP;
    }

    int Unfetch(sqlite3_int64 iOfst, void *p) override {
        assert(false);
        return SQLITE_IOERR_MMAP;
    }

  public:
    InnerDatabaseFile(std::unique_ptr<SQLite::Database> &&outer_db,
                      const std::string &inner_db_tablename_prefix, bool read_only)
        : outer_db_(std::move(outer_db)), inner_db_tablename_prefix_(inner_db_tablename_prefix),
          read_only_(read_only),
          select_pages_(*outer_db_,
                        "SELECT * FROM " + inner_db_tablename_prefix_ +
                            "pages WHERE page_idx >= ? AND page_idx < ? ORDER BY page_idx"),
          select_page_count_(*outer_db_, "SELECT COUNT(page_idx), SUM(page_idx) FROM " +
                                             inner_db_tablename_prefix_ + "pages"),
          begin_(*outer_db_, "BEGIN"), commit_(*outer_db_, "COMMIT") {
        methods_.iVersion = 1;
        assert(outer_db_->execAndGet("PRAGMA quick_check").getString() == "ok");
    }
};

// issue when write performance is prioritized over transaction safety / possible corruption
const char *UNSAFE_PRAGMAS =
    "PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA locking_mode=EXCLUSIVE";

class VFS : public SQLiteVFS::Wrapper {
  protected:
    // subclass may override inner_db_tablename_prefix_ with something encoding-specific, to
    // prevent confusion between different nested VFS encodings
    std::string inner_db_tablename_prefix_ = "inner_vfs_";
    std::string inner_db_filename_suffix_ = "-inner", outer_vfs_, last_error_;

    size_t szOsFile() override {
        return std::max(SQLiteVFS::Wrapper::szOsFile(), (size_t)wrapped_->szOsFile);
    }

    // test whether the db seems to have a nested inner db
    virtual bool IsOuterDB(SQLite::Database &db) {
        return db.tableExists(inner_db_tablename_prefix_ + "pages");
    }

    // create page table; called within a transaction, subclasses may override and add aux tables
    virtual void InitOuterDB(SQLite::Database &db) {
        SQLite::Statement check(db, "SELECT * FROM sqlite_master");
        if (check.executeStep()) {
            throw SQLite::Exception("expected empty database in which to create nested VFS",
                                    SQLITE_CANTOPEN);
        }
        std::vector<std::pair<const char *, const char *>> ddl = {
            {"CREATE TABLE ",
             "pages (page_idx INTEGER PRIMARY KEY, data BLOB NOT NULL, meta1 BLOB, meta2 BLOB)"}};
        for (const auto &p : ddl) {
            SQLite::Statement(db, p.first + inner_db_tablename_prefix_ + p.second).executeStep();
        }
    }

    virtual std::unique_ptr<SQLiteVFS::File>
    NewInnerDatabaseFile(const char *zName, std::unique_ptr<SQLite::Database> &&outer_db,
                         bool read_only) {
        return std::unique_ptr<SQLiteVFS::File>(
            new InnerDatabaseFile(std::move(outer_db), inner_db_tablename_prefix_, read_only));
    }

    int Open(const char *zName, sqlite3_file *pFile, int flags, int *pOutFlags) override {
        if (zName && zName[0]) {
            std::string sName(zName);
            if (flags & SQLITE_OPEN_MAIN_DB) {
                // strip inner_db_filename_suffix_ to get filename of outer database
                std::string outer_db_filename =
                    sName.size() > inner_db_filename_suffix_.size()
                        ? sName.substr(0, sName.size() - inner_db_filename_suffix_.size())
                        : "";
                if (outer_db_filename.empty() ||
                    sName.substr(outer_db_filename.size()) != inner_db_filename_suffix_) {
                    last_error_ = "inner database filename unexpectedly missing suffix " +
                                  inner_db_filename_suffix_;
                    return SQLITE_CANTOPEN_FULLPATH;
                }

                try {
                    // open outer database
                    std::unique_ptr<SQLite::Database> outer_db(
                        new SQLite::Database(outer_db_filename, flags, 0, outer_vfs_));
                    // need exclusive locking mode to protect InnerDatabaseFile::page_count_; see
                    // discussion in Lock()
                    outer_db->exec("PRAGMA locking_mode=EXCLUSIVE");
                    if (sqlite3_uri_boolean(zName, "outer_unsafe", 0)) {
                        outer_db->exec(UNSAFE_PRAGMAS);
                    }
                    auto outer_cache_size = sqlite3_uri_int64(zName, "outer_cache_size", 0);
                    if (outer_cache_size) {
                        outer_db->exec("PRAGMA cache_size=" + std::to_string(outer_cache_size));
                    }

                    // check flags vs whether this appears to be a nested VFS database
                    bool is_outer_db = IsOuterDB(*outer_db);
                    if (is_outer_db && (flags & SQLITE_OPEN_EXCLUSIVE)) {
                        return SQLITE_CANTOPEN;
                    }
                    if (!is_outer_db) {
                        if (!(flags & SQLITE_OPEN_CREATE)) {
                            return SQLITE_CANTOPEN;
                        }
                        auto outer_page_size = sqlite3_uri_int64(zName, "outer_page_size", 0);
                        if (outer_page_size) {
                            outer_db->exec("PRAGMA page_size=" + std::to_string(outer_page_size));
                        }
                        // page_size directive must precede auto_vacuum in order to be effective
                        outer_db->exec("PRAGMA auto_vacuum=INCREMENTAL");
                        SQLite::Transaction txn(*outer_db);
                        InitOuterDB(*outer_db);
                        txn.commit();
                    }

                    auto idbf = NewInnerDatabaseFile(zName, std::move(outer_db),
                                                     (flags & SQLITE_OPEN_READONLY));
                    idbf->InitHandle(pFile);
                    assert(pFile->pMethods);
                    idbf.release();
                    *pOutFlags = flags;
                    return SQLITE_OK;
                } catch (SQLite::Exception &exn) {
                    last_error_ = exn.getErrorStr();
                    _DBG << last_error_ << _EOL;
                    return SQLITE_CANTOPEN;
                }
            }
        }
        // Anything besides inner database file: let outer VFS handle it.
        // Remove (SQLITE_OPEN_WAL|SQLITE_OPEN_MAIN_JOURNAL) to prevent SQLite os_unix.c from
        // trying to copy owner/perms of the inner database file (which won't exist from its point
        // of view)
        //    see: https://sqlite.org/src/file?name=src/os_unix.c&ci=tip
        flags &= ~(SQLITE_OPEN_WAL | SQLITE_OPEN_MAIN_JOURNAL);
        assert(vfs_.szOsFile >= wrapped_->szOsFile);
        return wrapped_->xOpen(wrapped_, zName, pFile, flags, pOutFlags);
    }

    // Given user-provided db filename, use it as the outer db on the host filesystem, and append
    // a suffix as the inner db's filename (which won't actually exist on the host filesystem, but
    // xOpen() will recognize).
    int FullPathname(const char *zName, int nPathOut, char *zPathOut) {
        int rc = SQLiteVFS::Wrapper::FullPathname(zName, nPathOut, zPathOut);
        if (rc != SQLITE_OK) {
            return rc;
        }
        std::string outer_db_filename(zPathOut);
        if (outer_db_filename.size() > inner_db_filename_suffix_.size() &&
            outer_db_filename.substr(outer_db_filename.size() - inner_db_filename_suffix_.size()) ==
                inner_db_filename_suffix_) {
            last_error_ = "nested VFS database name mustn't end with " + inner_db_filename_suffix_;
            return SQLITE_CANTOPEN_FULLPATH;
        }
        std::string inner_db_filename = outer_db_filename + inner_db_filename_suffix_;
        if (inner_db_filename.size() >= nPathOut) {
            return SQLITE_TOOBIG;
        }
        strncpy(zPathOut, inner_db_filename.c_str(), nPathOut - 1);
        zPathOut[nPathOut] = 0;
        return SQLITE_OK;
    }

    int GetLastError(int nByte, char *zErrMsg) override {
        if (nByte && last_error_.size()) {
            strncpy(zErrMsg, last_error_.c_str(), nByte);
            zErrMsg[nByte - 1] = 0;
            return SQLITE_OK;
        }
        return SQLiteVFS::Wrapper::GetLastError(nByte, zErrMsg);
    }
};

} // namespace SQLiteNested

#undef _DBG
#undef _EOL
