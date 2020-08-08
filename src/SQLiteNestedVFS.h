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
#include "ThreadPool.h"
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
//   2. xSync() and xClose() are only called at moments when the inner database will be consistent
//      once previously written pages are flushed. That is, SQLite would never explicitly request
//      to sync inconsistent state of the main db file in the middle of a transaction.
class InnerDatabaseFile : public SQLiteVFS::File {
  protected:
    // RAII helper
    class StatementResetter {
      private:
        SQLite::Statement &stmt_;

      public:
        StatementResetter(SQLite::Statement &stmt) : stmt_(stmt) {}
        ~StatementResetter() { stmt_.tryReset(); }
    };

    std::unique_ptr<SQLite::Database> outer_db_;
    std::string inner_db_tablename_prefix_; // can be overridden in VFS, not here
    bool read_only_;

    /**********************************************************************************************
     * Database file geometry
     *********************************************************************************************/

    size_t page_size_ = 0, page_count_ = 0;

    // Update page_count_ (a mutable property of the inner db) based on some inspection of outer
    // db. Leave it at zero if the inner db is still new.
    //   1. Corruption is likely to occur if page_count_ somehow drifts from the actual outer db
    //      state, e.g. by a concurrent writer. We must redetect it every time we take read lock.
    //   2. Write() may update page_count_ to reflect newly written pages without calling this.
    SQLite::Statement select_page_count_;
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
                page_count_ = page_count;
            } else {
                assert(page_count == 0);
                page_count_ = 0;
            }
            assert(VerifyPageCount());
        }
        return page_count_;
    }

    // Update page_size_ (a property of the inner db, immutable once set on first write) based on
    // some inspection of outer db. Leave at zero if the inner db is still new. Default logic takes
    // the size of the data for the first page; subclasses may override.
    virtual size_t DetectPageSize() {
        if (!page_size_ && DetectPageCount()) {
            cursor_.Seek(1);
            int sz = cursor_.getColumn(1).getBytes();
            if (sz <= 0 || sz > 65536) {
                throw SQLite::Exception("invalid page size in nested VFS page table",
                                        SQLITE_CORRUPT);
            }
            page_size_ = (size_t)sz;
        }
        return page_size_;
    }

    bool VerifyPageCount() {
        // integrity check: page indices are sequential [1..page_count_]
        sqlite3_int64 pageno_sum =
            outer_db_->execAndGet("SELECT SUM(pageno) FROM " + inner_db_tablename_prefix_ + "pages")
                .getInt64();
        return pageno_sum == page_count_ * (page_count_ + 1) / 2;
    }

    int FileSize(sqlite3_int64 *pSize) override {
        try {
            FinishUpserts();
            *pSize = DetectPageSize() * DetectPageCount();
            return SQLITE_OK;
        } catch (std::exception &exn) {
            _DBG << exn.what() << _EOL;
            return SQLITE_IOERR;
        }
    }

    /**********************************************************************************************
     * Read() and helpers
     *********************************************************************************************/

    // To optimize sequential page reads, we usually keep a cursor open to the next page after the
    // one last read; this internal struct manages the state for that. We must be careful to reset
    // it at certain times (e.g. when writing a page) to ensure the open cursor can't be stale.
    struct ReadCursor {
        SQLite::Statement select_pages_;
        sqlite3_int64 pageno_ = 0;
        sqlite3_int64 sequential_ = 0, non_sequential_ = 0;

        ReadCursor(SQLite::Database &db, const std::string &inner_db_tablename_prefix)
            : select_pages_(db, "SELECT pageno, data, meta1, meta2 FROM " +
                                    inner_db_tablename_prefix +
                                    "pages WHERE pageno >= ? ORDER BY pageno") {}

        void Reset() {
            if (pageno_ > 0) {
                select_pages_.reset();
                pageno_ = 0;
            }
        }

        void Seek(sqlite3_int64 pageno) {
            assert(pageno > 0);
            if (pageno_ && pageno == pageno_ + 1) {
                sequential_++;
            } else {
                Reset();
                select_pages_.bind(1, pageno);
                non_sequential_++;
            }
            try {
                pageno_ = pageno;
                if (!select_pages_.executeStep()) {
                    throw SQLite::Exception("page missing", SQLITE_CORRUPT);
                }
            } catch (...) {
                Reset();
                throw;
            }
            assert(select_pages_.getColumn(0).isInteger());
            assert(select_pages_.getColumn(0).getInt64() == pageno);
            assert(select_pages_.getColumn(1).isBlob());
        }

        SQLite::Column getColumn(int idx) { return select_pages_.getColumn(idx); }
    };
    ReadCursor cursor_;
    sqlite3_int64 longest_read_ = 0;

    // Decode one page from the blob stored in the outer db (page_size_ bytes out). Override me!
    virtual void DecodePage(sqlite3_int64 pageno, const SQLite::Column &data,
                            const SQLite::Column &meta1, const SQLite::Column &meta2, void *dest) {
        if (!data.isBlob() || data.getBytes() != page_size_) {
            std::string errmsg = "page " + std::to_string(pageno) +
                                 " size = " + std::to_string(data.getBytes()) +
                                 " expected = " + std::to_string(page_size_);
            throw SQLite::Exception(errmsg, SQLITE_CORRUPT);
        }
        memcpy(dest, data.getBlob(), page_size_);
    }

    int Read(void *zBuf, int iAmt, sqlite3_int64 iOfst) override {
        try {
            assert(iAmt >= 0);
            if (iAmt == 0) {
                return SQLITE_OK;
            }
            FinishUpserts();
            if (!DetectPageSize()) { // file is empty
                memset(zBuf, 0, iAmt);
                return iAmt > 0 ? SQLITE_IOERR_SHORT_READ : SQLITE_OK;
            }
            assert(page_size_ <= 65536);
            if (iOfst >= page_size_ && iOfst % page_size_) { // non-aligned read
                return SQLITE_IOERR_READ;
            }

            // calculate page number range
            sqlite3_int64 first_page = 1 + iOfst / page_size_,
                          last_page = first_page + (iAmt - 1) / page_size_;
            int sofar = 0;

            // scan them
            for (sqlite3_int64 pageno = first_page; pageno <= last_page; ++pageno) {
                cursor_.Seek(pageno);
                int page_ofs = sofar == 0 ? iOfst % page_size_ : 0;
                int desired = std::min(iAmt - sofar, (int)page_size_ - page_ofs);
                if (page_ofs == 0 && desired == page_size_) {
                    // aligned read of a whole page
                    DecodePage(pageno, cursor_.getColumn(1), cursor_.getColumn(2),
                               cursor_.getColumn(3), (uint8_t *)zBuf + sofar);
                } else {
                    // non-aligned read (happens when SQLite reads the db header)
                    // TODO: optimize with incremental blob I/O, if we know page 1 is "plaintext"
                    assert(pageno == 1);
                    std::vector<char> pagebuf(page_size_, 0);
                    DecodePage(pageno, cursor_.getColumn(1), cursor_.getColumn(2),
                               cursor_.getColumn(3), pagebuf.data());
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
            longest_read_ = std::max((sqlite3_int64)iAmt, longest_read_);
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

    /**********************************************************************************************
     * Write() and helpers
     *********************************************************************************************/

    // On the first Write() we open a transaction with the outer db as a buffer for this and
    // subsequent writes, which we commit upon Sync() or Close(). We never explicitly rollback this
    // transaction, although rollback may occur if the process/host crashes. By Assumption 2 above,
    // inner db should always recover to a consistent state.
    std::unique_ptr<SQLite::Transaction> txn_;
    std::unique_ptr<SQLite::Statement> insert_page_, update_page_, delete_pages_;
    void begin() {
        assert(!read_only_);
        if (!txn_) {
            txn_.reset(new SQLite::Transaction(*outer_db_));
        }
    }

    // holds state of page-encoding background job
    struct EncodeJob {
        // job inputs
        sqlite3_int64 pageno = -1;
        std::string page;
        bool insert = false;

        // job outputs
        sqlite3_int64 meta1, meta2;
        bool meta1null = true, meta2null = true;
        const char *encoded_page = nullptr;
        size_t encoded_page_size = 0;

        std::string errmsg;

        virtual ~EncodeJob() = default;
        virtual void Execute() noexcept {
            // Execute the job, possibly on a worker thread; for subclass to override
            encoded_page = page.data();
            encoded_page_size = page.size();
        }
    };

    // construct EncodeJob (may be overridden by subclass with additional state)
    virtual std::unique_ptr<EncodeJob> NewEncodeJob() {
        return std::unique_ptr<EncodeJob>(new EncodeJob);
    }
    virtual void InitEncodeJob(EncodeJob &job, sqlite3_int64 pageno, const void *page_data) {
        // Initialize job in main thread
        job.pageno = pageno;
        job.insert = pageno > page_count_;
        if (page_data) {
            // copy page data for background-encoding
            job.page.assign((const char *)page_data, page_size_);
        } else {
            job.page.assign(page_size_, 0);
        }

        job.meta1null = job.meta2null = true;
        job.encoded_page = nullptr;
        job.encoded_page_size = 0;
        job.errmsg.clear();
    }

    // pool of EncodeJob instances, to minimize allocations
    std::vector<std::unique_ptr<EncodeJob>> encode_job_pool_;
    std::mutex encode_job_pool_mutex_;

    ThreadPool thread_pool_;
    std::string upsert_errmsg_;

    // enqueue page encoding+upsert on thread pool
    void EnqueueUpsert(sqlite_int64 pageno, const void *page) {
        try {
            std::unique_ptr<EncodeJob> job;
            {
                // fetch from buffer pool
                std::lock_guard<std::mutex> lock(encode_job_pool_mutex_);
                if (!encode_job_pool_.empty()) {
                    encode_job_pool_.back().swap(job);
                    encode_job_pool_.pop_back();
                    assert(job);
                }
            }
            if (!job) {
                NewEncodeJob().swap(job);
            }
            InitEncodeJob(*job, pageno, page);
            assert(job->insert ? pageno == page_count_ + 1 : pageno <= page_count_);
            page_count_ += job->insert ? 1 : 0;
            // use ThreadPool to run encoding jobs in parallel and upsert jobs serially
            thread_pool_.Enqueue(
                job.release(),
                [](void *job) noexcept {
                    ((EncodeJob *)job)->Execute();
                    return job;
                },
                [this](void *job) noexcept { this->ExecuteUpsert((EncodeJob *)job); });

        } catch (std::exception &exn) {
            _DBG << exn.what() << _EOL;
            page_count_ = 0; // to be redetected
            throw;
        }
    }

    // perform upsert operation in worker thread; ThreadPool runs these serially in enqueued order
    void ExecuteUpsert(EncodeJob *pjob) noexcept {
        std::unique_ptr<EncodeJob> job(pjob);
        try {
            if (!job->errmsg.empty()) {
                throw std::runtime_error(job->errmsg);
            }
            auto &upsert = job->insert ? insert_page_ : update_page_;
            upsert->clearBindings();
            upsert->bindNoCopy(1, job->encoded_page, job->encoded_page_size);
            if (!job->meta1null) {
                upsert->bind(2, job->meta1);
            }
            if (!job->meta2null) {
                upsert->bind(3, job->meta2);
            }
            upsert->bind(4, job->pageno);
            StatementResetter resetter(*upsert);
            if (upsert->exec() != 1) {
                throw std::runtime_error("unexpected result from page upsert");
            }

            {
                // return to buffer pool
                std::lock_guard<std::mutex> lock(encode_job_pool_mutex_);
                encode_job_pool_.emplace_back(job.release());
            }
        } catch (std::exception &exn) {
            std::string errmsg =
                std::string("background page encoding/upsert failed: ") + exn.what();
            _DBG << errmsg << _EOL;
            if (upsert_errmsg_.empty()) {
                upsert_errmsg_ = errmsg;
            }
        }
    }

    // wait for background upserts to complete + raise any error message
    void FinishUpserts(bool ignore_error = false) {
        thread_pool_.Barrier();
        if (!ignore_error && !upsert_errmsg_.empty()) {
            throw SQLite::Exception(upsert_errmsg_, SQLITE_IOERR_WRITE);
        }
    }

    int Write(const void *zBuf, int iAmt, sqlite3_int64 iOfst) override {
        assert(iAmt >= 0);
        assert(!read_only_);
        try {
            cursor_.Reset();
            if (!iAmt) {
                return SQLITE_OK;
            }
            if (!DetectPageSize()) {
                // db is new; assume caller is writing one whole page
                page_size_ = iAmt;
            }
            if (iAmt % page_size_ || iOfst % page_size_) {
                throw SQLite::Exception("non-aligned write", SQLITE_IOERR_WRITE);
            }

            if (!insert_page_) {
                insert_page_.reset(new SQLite::Statement(
                    *outer_db_, "INSERT INTO " + inner_db_tablename_prefix_ +
                                    "pages(data,meta1,meta2,pageno) VALUES(?,?,?,?)"));
            }
            if (!update_page_) {
                update_page_.reset(new SQLite::Statement(
                    *outer_db_, "UPDATE " + inner_db_tablename_prefix_ +
                                    "pages SET data=?, meta1=?, meta2=? WHERE pageno=?"));
            }

            begin();
            while (iOfst / page_size_ > DetectPageCount()) {
                // write past current EOF; fill "hole" with zeroes
                EnqueueUpsert(sqlite3_int64(page_count_ + 1), nullptr);
            }

            // upsert each provided page
            int sofar = 0;
            while (sofar < iAmt) {
                assert(!((iOfst + sofar) % page_size_));
                assert(!((iAmt - sofar) % page_size_));
                EnqueueUpsert(1 + (iOfst + sofar) / page_size_, (uint8_t *)zBuf + sofar);
                sofar += page_size_;
            }
            assert(sofar == iAmt);
            return SQLITE_OK;
        } catch (std::exception &exn) {
            _DBG << exn.what() << _EOL;
            FinishUpserts(true);
            page_count_ = 0; // to be redetected
            return SQLITE_IOERR_WRITE;
        }
    }

    int Truncate(sqlite3_int64 size) override {
        assert(!read_only_);
        try {
            cursor_.Reset();
            FinishUpserts();
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
                                                          "pages WHERE pageno > ?"));
            }
            delete_pages_->bind(1, new_page_count);
            StatementResetter resetter(*delete_pages_);
            begin();
            delete_pages_->exec();
            page_count_ = new_page_count;
            assert(VerifyPageCount());
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
            FinishUpserts();
        } catch (std::exception &exn) {
            _DBG << exn.what() << _EOL;
            page_count_ = 0; // to be redetected
            return SQLITE_IOERR_WRITE;
        }
        assert(page_count_ == 0 || VerifyPageCount());
        try {
            if (txn_) {
                txn_->commit();
                txn_.reset();
            }
        } catch (SQLite::Exception &exn) {
            _DBG << exn.what() << _EOL;
            page_count_ = 0; // to be redetected
            return exn.getErrorCode();
        }
        return SQLITE_OK;
    }

    /**********************************************************************************************
     * etc.
     *********************************************************************************************/

    int Lock(int eLock) override {
        /*
        No-op: currently we hardcode locking_mode=EXCLUSIVE for the outer db, so there's no need to
        respond to inner db lock calls. This can be relaxed in the future by:
          - when we get Lock(SQLITE_LOCK_SHARED): open some read cursor on the outer db
          - when we get Lock(SQLITE_LOCK_EXCLUSIVE): BEGIN EXCLUSIVE txn on outer db
          - when we get xUnlock(SQLITE_LOCK_SHARED): Sync() if needed (usually will have already)
          - when we get xUnlock(SQLITE_LOCK_NONE): close the read cursor we'd opened
        Holding the open cursor is supposed to ensure outer db lock only downgrades from EXCLUSIVE
        to SHARED upon txn commit, rather than releasing entirely. Testing of all this will be
        tricky, so we've punted on it for now.
        */
        return SQLITE_OK;
    }
    int Unlock(int eLock) override {
        if (eLock == SQLITE_LOCK_NONE) {
            // wipe internal state that's liable to go stale after relinquishing read lock
            page_count_ = 0;
            try {
                cursor_.Reset();
            } catch (SQLite::Exception &exn) {
                _DBG << exn.what() << _EOL;
                return exn.getErrorCode();
            }
        }
        return SQLITE_OK;
    }
    int CheckReservedLock(int *pResOut) override {
        *pResOut = 0;
        return SQLITE_OK;
    }
    int FileControl(int op, void *pArg) override { return SQLITE_NOTFOUND; }
    int SectorSize() override { return 0; }
    int DeviceCharacteristics() override {
        // we can support SQLITE_IOCAP_SEQUENTIAL and SQLITE_IOCAP_SAFE_APPEND, but these only help
        // journals rather than the main database file (see SQLite pager.c)
        return SQLITE_IOCAP_ATOMIC | SQLITE_IOCAP_POWERSAFE_OVERWRITE;
    }

    int Close() override {
        try {
            cursor_.Reset();
            if (!read_only_) {
                int rc;
                if ((rc = Sync(0)) != SQLITE_OK) { // includes FinishUpserts
                    return rc;
                }
                outer_db_->exec("PRAGMA incremental_vacuum");
            }
        } catch (SQLite::Exception &exn) {
            _DBG << exn.what() << _EOL;
            return exn.getErrorCode();
        }
        if (cursor_.sequential_ + cursor_.non_sequential_) {
            _DBG << "reads sequential: " << cursor_.sequential_
                 << " non-sequential: " << cursor_.non_sequential_ << " longest: " << longest_read_
                 << _EOL;
        }
        return SQLiteVFS::File::Close();
    }

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
                      const std::string &inner_db_tablename_prefix, bool read_only, size_t threads)
        : outer_db_(std::move(outer_db)), inner_db_tablename_prefix_(inner_db_tablename_prefix),
          read_only_(read_only),
          // MAX(pageno) instead of COUNT(pageno) because the latter would trigger table scan
          select_page_count_(*outer_db_, "SELECT IFNULL(MAX(pageno), 0) FROM " +
                                             inner_db_tablename_prefix_ + "pages"),
          cursor_(*outer_db_, inner_db_tablename_prefix), thread_pool_(threads, threads * 3) {
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
             "pages (pageno INTEGER PRIMARY KEY, data BLOB NOT NULL, meta1 BLOB, meta2 BLOB)"}};
        for (const auto &p : ddl) {
            SQLite::Statement(db, p.first + inner_db_tablename_prefix_ + p.second).executeStep();
        }
    }

    virtual std::unique_ptr<SQLiteVFS::File>
    NewInnerDatabaseFile(const char *zName, std::unique_ptr<SQLite::Database> &&outer_db,
                         bool read_only, size_t threads) {
        return std::unique_ptr<SQLiteVFS::File>(new InnerDatabaseFile(
            std::move(outer_db), inner_db_tablename_prefix_, read_only, threads));
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

                // TODO: URI-encode outer_db_filename
                std::string outer_db_uri = "file:" + outer_db_filename;
                bool unsafe = sqlite3_uri_boolean(zName, "outer_unsafe", 0);
                if (unsafe) {
                    outer_db_uri += "?nolock=1&psow=1";
                } else if (sqlite3_uri_boolean(zName, "immutable", 0)) {
                    outer_db_uri += "?immutable=1";
                }

                try {
                    // open outer database
                    std::unique_ptr<SQLite::Database> outer_db(new SQLite::Database(
                        outer_db_uri, flags | SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_URI, 0,
                        outer_vfs_));
                    // see comment in Lock() about possibe future relaxation of exclusive locking
                    outer_db->exec("PRAGMA locking_mode=EXCLUSIVE");
                    if (unsafe) {
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

                    auto threads = sqlite3_uri_int64(zName, "threads", 1);
                    if (threads < 0) {
                        threads = sqlite3_int64(std::thread::hardware_concurrency()) - 1;
                    }
                    if (threads < 1) {
                        threads = 1;
                    }

                    auto idbf =
                        NewInnerDatabaseFile(zName, std::move(outer_db),
                                             (flags & SQLITE_OPEN_READONLY), (size_t)threads);
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
        // Remove SQLITE_OPEN_MAIN_JOURNAL to prevent SQLite os_unix.c from trying to copy owner &
        // perms of the inner database file, which doesn't exist from its POV. We assume os_unix.c
        // doesn't do anything else special for SQLITE_OPEN_MAIN_JOURNAL that might be required for
        // transaction safety! (The inner journal file is usually unused anyway.)
        // see: https://sqlite.org/src/file?name=src/os_unix.c&ci=tip
        // Note it has an assert that flags has -some- eType set; so, in case the host SQLite has
        // assertions on, we add SQLITE_OPEN_SUBJOURNAL when we remove SQLITE_OPEN_MAIN_JOURNAL.
        // The same issue will affect SQLITE_OPEN_WAL if/when we try to suport WAL mode.
        if (flags & SQLITE_OPEN_MAIN_JOURNAL) {
            flags &= ~SQLITE_OPEN_MAIN_JOURNAL;
            flags |= SQLITE_OPEN_SUBJOURNAL;
        }
        assert(vfs_.szOsFile >= wrapped_->szOsFile);
        return wrapped_->xOpen(wrapped_, zName, pFile, flags, pOutFlags);
    }

    // Given user-provided db filename, use it as the outer db on the host filesystem, and append
    // a suffix as the inner db's filename (which won't actually exist on the host filesystem, but
    // xOpen() will recognize).
    int FullPathname(const char *zName, int nPathOut, char *zPathOut) override {
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
