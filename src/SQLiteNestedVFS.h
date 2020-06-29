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
    size_t page_size_ = 0, page_count_ = 0;
    // cached statements; must be declared after outer_db_ so that they're destructed first
    std::unique_ptr<SQLite::Transaction> txn_;
    SQLite::Statement select_pages_, select_page_count_;
    std::unique_ptr<SQLite::Statement> insert_page_, update_page_, delete_pages_;

    // On the first Write() we open a transaction with the outer db as a buffer for this and
    // subsequent writes, which we commit upon Sync() or Close(). We never explicitly rollback this
    // transaction, although rollback may occur if the process/host crashes. By Assumption 2 above,
    // inner db should always recover to a consistent state.
    void begin() {
        assert(!read_only_);
        if (!txn_) {
            txn_.reset(new SQLite::Transaction(*outer_db_));
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
            std::string errmsg = "page " + std::to_string(page_idx) +
                                 " size = " + std::to_string(data.getBytes()) +
                                 " expected = " + std::to_string(page_size_);
            throw SQLite::Exception(errmsg, SQLITE_CORRUPT);
        }
        memcpy(dest, data.getBlob(), page_size_);
    }

    ThreadPool thread_pool_;

    // holds state of page-encoding background job
    struct EncodeJob {
        // job inputs
        sqlite3_int64 page_idx = -1;
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
    virtual void InitEncodeJob(EncodeJob &job, sqlite3_int64 page_idx, const void *page_data) {
        // Initialize job in main thread
        job.page_idx = page_idx;
        job.insert = page_idx == page_count_;
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

    std::string upsert_errmsg_;

    // enqueue page encoding+upsert on thread pool
    void EnqueueUpsert(sqlite_int64 page_idx, const void *page) {
        assert(page_idx <= page_count_);
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
            InitEncodeJob(*job, page_idx, page);
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
            upsert->bind(4, job->page_idx);
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

#ifndef NDEBUG
    sqlite3_int64 last_page_read_ = -999, sequential_reads_ = 0, non_sequential_reads_ = 0,
                  longest_read_ = 0;
#endif

    int Close() override {
        if (!read_only_) {
            int rc;
            if ((rc = Sync(0)) != 0) { // includes FinishUpserts
                return rc;
            }
            try {
                outer_db_->exec("PRAGMA incremental_vacuum");
            } catch (SQLite::Exception &exn) {
                _DBG << exn.what() << _EOL;
                return exn.getErrorCode();
            }
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
        if (!iAmt) {
            return SQLITE_OK;
        }
        try {
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
                                    "pages(data,meta1,meta2,page_idx) VALUES(?,?,?,?)"));
            }
            if (!update_page_) {
                update_page_.reset(new SQLite::Statement(
                    *outer_db_, "UPDATE " + inner_db_tablename_prefix_ +
                                    "pages SET data=?, meta1=?, meta2=? WHERE page_idx=?"));
            }

            begin();
            while (iOfst / page_size_ > DetectPageCount()) {
                // write past current EOF; fill "hole" with zeroes
                EnqueueUpsert((sqlite3_int64)page_count_, nullptr);
            }

            // upsert each provided page
            int sofar = 0;
            while (sofar < iAmt) {
                assert(!((iOfst + sofar) % page_size_));
                assert(!((iAmt - sofar) % page_size_));
                EnqueueUpsert((iOfst + sofar) / page_size_, (uint8_t *)zBuf + sofar);
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
                                                          "pages WHERE page_idx >= ?"));
            }
            delete_pages_->bind(1, new_page_count);
            StatementResetter resetter(*delete_pages_);
            begin();
            delete_pages_->exec();
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
            FinishUpserts();
        } catch (std::exception &exn) {
            _DBG << exn.what() << _EOL;
            page_count_ = 0; // to be redetected
            return SQLITE_IOERR_WRITE;
        }
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
                      const std::string &inner_db_tablename_prefix, bool read_only, size_t threads)
        : outer_db_(std::move(outer_db)), inner_db_tablename_prefix_(inner_db_tablename_prefix),
          read_only_(read_only),
          select_pages_(*outer_db_,
                        "SELECT * FROM " + inner_db_tablename_prefix_ +
                            "pages WHERE page_idx >= ? AND page_idx < ? ORDER BY page_idx"),
          select_page_count_(*outer_db_, "SELECT COUNT(page_idx), SUM(page_idx) FROM " +
                                             inner_db_tablename_prefix_ + "pages"),
          thread_pool_(threads, threads * 2) {
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

                try {
                    // open outer database
                    std::unique_ptr<SQLite::Database> outer_db(new SQLite::Database(
                        outer_db_filename, flags | SQLITE_OPEN_NOMUTEX, 0, outer_vfs_));
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
