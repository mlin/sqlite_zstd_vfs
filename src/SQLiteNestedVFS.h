/**
 * SQLite VFS that stores the pages of an "inner" main database file in an "outer" database. See:
 *   https://www.sqlite.org/fileformat.html
 *   https://sqlite.org/zipvfs/doc/trunk/www/howitworks.wiki
 *
 * Instead of writing pages to a calculated file offset, upserts them into the outer pages table,
 * from which they can later be queried as well. This implementation stores the pages as-is which
 * is useless, but subclasses can easily layer in compression and/or encryption codecs.
 *
 * Uses thread pools so that (i) page encoding and writeback occur on background threads, and (ii)
 * page reads and decoding can be "prefetched" in the background during detected sequential scans.
 * These schemes are each a bit complex but at least they're separate: when asked to read a page,
 * we first wait for any outstanding writes to finish, and vice-versa.
 */
#pragma once

#include "SQLiteCpp/SQLiteCpp.h"
#include "SQLiteVFS.h"
#include "ThreadPool.h"
#include <atomic>
#include <chrono>
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
    std::string inner_db_pages_table_;
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
    bool page1plain_ = true; // subclass override if page 1 isn't stored "plaintext"
    virtual size_t DetectPageSize() {
        if (!page_size_ && DetectPageCount()) {
            assert(page1plain_);
            SQLite::Statement stmt(*outer_db_, "SELECT length(data) FROM " + inner_db_pages_table_ +
                                                   " WHERE pageno = 1");
            sqlite3_int64 sz = -1;
            if (stmt.executeStep()) {
                sz = stmt.getColumn(0).getInt64();
            }
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
        sqlite3_int64 pagenosum =
            outer_db_->execAndGet("SELECT SUM(pageno) FROM " + inner_db_pages_table_).getInt64();
        return pagenosum == page_count_ * (page_count_ + 1) / 2;
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

    // Holds state for a background page fetch: reading it from the outer database and decoding it
    // on a background thread, to hide decompression time while reading sequential pages.
    // Several tricky bits:
    // 1. Interactions with the outer database must be serialized, while decoding may parallelize.
    // 2. After seeking to a page in the outer database, keep the cursor open so that it can be
    //    reused if we're later asked for the next sequential page.
    // 3. When the main thread wants a page that isn't being prefetched, let it cut in line.
    // 4. When the main thread wants a page that's currently being prefetched, have it wait.
    // 5. Little thread synchronization overhead is affordable, as Zstandard decompression of a
    //    SQLite page (4-64 KiB) takes only 1-10 microseconds -- about 10x time to memcpy.
    // Subclasses may in turn subclass this to add their own logic to SeekCursor and DecodePage.
    struct PageFetchJob {
        enum State { NEW, QUEUE, WIP, DONE };
        // NEW = idle, QUEUE = ready & waiting, WIP = work in progress, DONE = complete or error
        // QUEUE=>WIP is the only contentious transition; background threads and the main thread
        // may all race (with an atomic CAS) to start the job. The winner later performs the
        // WIP=>DONE transition. Other transitions are only performed by the main thread.
        std::atomic<State> _state;
        inline State GetState() const { return _state.load(std::memory_order_acquire); }
        inline void PutState(State st) { _state.store(st, std::memory_order_release); }
        inline bool TransitionState(State expected, State desired) {
            return _state.compare_exchange_strong(expected, desired, std::memory_order_acq_rel);
        }
        std::string errmsg; // nonempty = error

        sqlite3_int64 pageno = 0; // desired page
        size_t page_size;
        void *dest = nullptr;        // If non-null, decode page directly here (foreground fetch)
        std::vector<char> decodebuf; // otherwise decode into this buffer (background fetch)

        SQLite::Statement cursor;        // outer db cursor
        sqlite3_int64 cursor_pageno = 0; // if >0 then cursor is currently on this page
        unsigned long long last_op = 0;  // "time" of the cursor's last use
        bool was_sequential = false;

        const void *src = nullptr; // outer db page to be decoded
        int src_size = 0;

        // counters
        sqlite3_int64 sequential = 0, non_sequential = 0;
        std::chrono::nanoseconds t_seek = std::chrono::nanoseconds::zero(),
                                 t_decode = std::chrono::nanoseconds::zero();

        PageFetchJob(InnerDatabaseFile &that)
            : cursor(*(that.outer_db_), "SELECT pageno, data, meta1, meta2 FROM " +
                                            that.inner_db_pages_table_ +
                                            " WHERE pageno >= ? ORDER BY pageno"),
              page_size(that.page_size_) {
            PutState(State::NEW);
        }

        virtual ~PageFetchJob() {}

        inline void *EffectiveDest() {
            return dest ? dest : (decodebuf.resize(page_size), decodebuf.data());
        }

        // prepare job for reuse (keep cursor & decodebuf)
        virtual void Renew() {
            PutState(State::NEW);
            pageno = 0;
            dest = nullptr;
            src = nullptr;
            src_size = 0;
            errmsg.clear();
        }

        // reset the outer database cursor; should be done when it otherwise might go stale.
        virtual void ResetCursor() {
            if (cursor_pageno > 0) {
                cursor.reset();
                cursor_pageno = 0;
            }
            last_op = 0;
        }

        // move cursor to pageno, by next() if possible, otherwise from scratch. these operations
        // must be serialized, as they deal with the outer db.
        virtual void SeekCursor() {
#ifndef NDEBUG
            auto t0 = std::chrono::high_resolution_clock::now();
#endif
            assert(pageno > 0);
            assert(errmsg.empty() && !src && !src_size);
            bool ans = false;
            if (cursor_pageno != pageno) {
                if (cursor_pageno + 1 == pageno && cursor_pageno) {
                    was_sequential = true;
#ifndef NDEBUG
                    sequential++;
#endif
                    ans = true;
                } else {
                    ResetCursor();
                    cursor.bind(1, pageno);
                    was_sequential = false;
#ifndef NDEBUG
                    non_sequential++;
#endif
                }
                if (!cursor.executeStep() || cursor.getColumn(0).getInt64() != pageno) {
                    throw SQLite::Exception("missing page " + std::to_string(pageno),
                                            SQLITE_CORRUPT);
                }
                cursor_pageno = pageno;
            }
            SQLite::Column data = cursor.getColumn(1);
            assert(data.getName() == std::string("data"));
            src = data.getBlob();
            src_size = data.getBytes();
            if (src_size && (!src || !data.isBlob())) {
                throw SQLite::Exception("corrupt page " + std::to_string(pageno), SQLITE_CORRUPT);
            }
#ifndef NDEBUG
            t_seek += std::chrono::high_resolution_clock::now() - t0;
#endif
        }

        // Decode one page from the blob stored in the outer db. Override me!
        // May parallelize with other jobs.
        virtual void DecodePage() {
#ifndef NDEBUG
            auto t0 = std::chrono::high_resolution_clock::now();
#endif
            if (src_size != page_size) {
                throw SQLite::Exception("page " + std::to_string(pageno) +
                                            " size = " + std::to_string(src_size) +
                                            " expected = " + std::to_string(page_size),
                                        SQLITE_CORRUPT);
            }
            memcpy(EffectiveDest(), src, src_size);
#ifndef NDEBUG
            t_decode += std::chrono::high_resolution_clock::now() - t0;
#endif
        }

        virtual void Execute(std::unique_lock<std::mutex> &seek_lock) noexcept {
            assert(GetState() == State::WIP);
            assert(seek_lock.owns_lock());
            try {
                SeekCursor();
                seek_lock.unlock();
                assert(cursor_pageno == pageno);
                DecodePage();
            } catch (std::exception &exn) {
                errmsg = exn.what();
                cursor_pageno = 0;
            }
            PutState(State::DONE);
        }
    };

    // Override me!
    virtual std::unique_ptr<PageFetchJob> NewPageFetchJob() {
        return std::unique_ptr<PageFetchJob>(new PageFetchJob(*this));
    }

    const int MAX_FETCH_CURSORS = 4;
    int max_fetch_jobs_;
    ThreadPool fetch_thread_pool_;
    std::vector<std::unique_ptr<PageFetchJob>> fetch_jobs_;
    std::mutex seek_lock_;
    std::atomic<bool> seek_interrupt_;

    unsigned long long read_opcount_ = 0, prefetch_wins_ = 0, prefetch_wasted_ = 0;
    sqlite3_int64 longest_read_ = 0;

    void *BackgroundFetchJob(void *ctx) {
        PageFetchJob *job = (PageFetchJob *)ctx;
        std::unique_lock<std::mutex> seek_lock(seek_lock_);
        while (seek_interrupt_.load(std::memory_order_relaxed)) {
            // give way to main thread trying to take seek_lock
            seek_lock.unlock();
            std::this_thread::yield();
            seek_lock.lock();
        }
        if (!job->TransitionState(PageFetchJob::State::QUEUE, PageFetchJob::State::WIP)) {
            return nullptr; // they took our job!!!!
        }
        job->Execute(seek_lock);
        return nullptr;
    }

    // Read page #pageno into dest, if possible by using a previously-scheduled prefetch.
    void Read1Page(void *dest, sqlite3_int64 pageno) {
        if (read_opcount_ == ULLONG_MAX) { // pedantic
            PrefetchBarrier();
            read_opcount_ = 0;
        }

        // Is there already a background job to prefetch the desired page?
        PageFetchJob *job = nullptr;
        bool foreground = false;
        for (auto job_i = fetch_jobs_.begin(); job_i != fetch_jobs_.end(); job_i++) {
            if ((*job_i)->pageno == pageno) {
                assert((*job_i)->GetState() > PageFetchJob::State::NEW);
                job = job_i->get();
                // If yes & it hasn't yet started, race to run it here in the foreground
                foreground =
                    job->TransitionState(PageFetchJob::State::QUEUE, PageFetchJob::State::WIP);
                break;
            }
        }

        // If no outstanding prefetch, create one; preferably using a pre-positioned cursor...
        if (!job) {
            for (auto job_i = fetch_jobs_.begin(); job_i != fetch_jobs_.end(); job_i++) {
                auto st = (*job_i)->GetState();
                if (st == PageFetchJob::State::NEW || st == PageFetchJob::State::DONE) {
                    if ((*job_i)->cursor_pageno + 1 == pageno) {
                        job = job_i->get();
                        break;
                    }
                    // ...or a cursor that isn't positioned anywhere useful, or lastly the LRU
                    if (!job || (*job_i)->cursor_pageno == 0 || job->last_op > (*job_i)->last_op) {
                        job = job_i->get();
                    }
                }
            }
            if (!job || (fetch_jobs_.size() < MAX_FETCH_CURSORS &&
                         job->cursor_pageno + 1 != pageno && job->cursor_pageno)) {
                assert(fetch_jobs_.size() < MAX_FETCH_CURSORS);
                fetch_jobs_.push_back(NewPageFetchJob());
                job = fetch_jobs_.back().get();
            }
#ifndef NDEBUG
            if (job->GetState() == PageFetchJob::State::DONE && job->cursor_pageno) {
                ++prefetch_wasted_;
            }
#endif
            job->Renew();
            job->pageno = pageno;
            job->dest = dest;
            job->PutState(PageFetchJob::State::WIP);
            foreground = true;
        }

        // If a background thread hasn't yet started the job, run it immediately here. Cut in line
        // for the seek lock and read the page directly onto this processor die.
        if (foreground) {
            seek_interrupt_.store(true, std::memory_order_relaxed);
            std::unique_lock<std::mutex> seek_lock(seek_lock_);
            seek_interrupt_.store(false, std::memory_order_relaxed);
            job->Execute(seek_lock);
            assert(job->GetState() == PageFetchJob::State::DONE);
        } else {
            // Semi-busy-wait for job to finish
            while (job->GetState() < PageFetchJob::State::DONE) {
                std::this_thread::yield();
            }
#ifndef NDEBUG
            ++prefetch_wins_;
#endif
        }

        if (!job->errmsg.empty()) {
            _DBG << job->errmsg << _EOL;
            job->Renew();
            throw SQLite::Exception(job->errmsg, SQLITE_IOERR_READ);
        }

        if (job->dest != dest) {
            // if data had been prefetched, copy it into place
            assert(!job->dest);
            memcpy(dest, job->decodebuf.data(), page_size_);
        }
        job->last_op = ++read_opcount_;
        job->Renew();

        // if the cursor had previously read pageno-1, use it to initiate prefetch of pageno+1
        if (job->was_sequential && max_fetch_jobs_ > 1 && pageno < page_count_) {
            int active_jobs = 0;
            for (auto job_i = fetch_jobs_.begin(); job_i != fetch_jobs_.end(); job_i++) {
                if (job_i->get() != job) {
                    auto st = (*job_i)->GetState();
                    if (st == PageFetchJob::State::QUEUE || st == PageFetchJob::State::WIP) {
                        ++active_jobs;
                    }
                }
            }
            // always leave at least one slot free for a foreground read to use
            if (active_jobs + 2 <= max_fetch_jobs_) {
                job->pageno = pageno + 1;
                job->PutState(PageFetchJob::State::QUEUE);
                fetch_thread_pool_.Enqueue(
                    job, [this](void *job) { return this->BackgroundFetchJob(job); }, nullptr);
            } else {
                assert(active_jobs + 1 == max_fetch_jobs_);
                // even if we don't have capacity for background-prefetch, the open cursor may yet
                // expedite a subsequent read of pageno+1.
            }
        }
    }

    void ReadPlainPage1(void *zBuf, int iAmt, int iOfst) {
        assert(page1plain_ && iOfst + iAmt <= page_size_);
        sqlite3_blob *pBlob = nullptr;
        int rc = sqlite3_blob_open(outer_db_->getHandle(), "main", inner_db_pages_table_.c_str(),
                                   "data", 1, 0, &pBlob);
        if (rc != SQLITE_OK) {
            throw SQLite::Exception("couldn't open page 1 blob", rc);
        }
        assert(sqlite3_blob_bytes(pBlob) == page_size_);
        rc = sqlite3_blob_read(pBlob, zBuf, iAmt, iOfst);
        if (rc != SQLITE_OK) {
            sqlite3_blob_close(pBlob);
            throw SQLite::Exception("couldn't read page 1 blob", rc);
        }
        rc = sqlite3_blob_close(pBlob);
        if (rc != SQLITE_OK) {
            throw SQLite::Exception("couldn't close page 1 blob", rc);
        }
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
            std::vector<char> pagebuf;

            // scan them
            for (sqlite3_int64 pageno = first_page; pageno <= last_page; ++pageno) {
                int page_ofs = sofar == 0 ? iOfst % page_size_ : 0;
                int desired = std::min(iAmt - sofar, (int)page_size_ - page_ofs);
                if (pageno == 1 && page1plain_) {
                    // optimized read of "plaintext" page 1, possibly including non-aligned
                    // reads (SQLite reading just the header or version number)
                    ReadPlainPage1(zBuf, desired, page_ofs);
                } else {
                    bool partial = page_ofs != 0 || desired != page_size_;
                    void *dest = (char *)zBuf + sofar;
                    Read1Page((partial ? (pagebuf.resize(page_size_), pagebuf.data()) : dest),
                              pageno);
                    if (partial) {
                        memcpy(dest, pagebuf.data() + page_ofs, desired);
                    }
                }
                sofar += desired;
                assert(sofar <= iAmt);
            }

            // finish up
            if (sofar < iAmt) {
                memset((char *)zBuf + sofar, 0, iAmt - sofar);
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

    void PrefetchBarrier() {
        // Abort prefetch jobs that haven't started yet
        seek_interrupt_.store(true, std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> seek_lock(seek_lock_);
            seek_interrupt_.store(false, std::memory_order_relaxed);
            for (auto &job : fetch_jobs_) {
                if (job->TransitionState(PageFetchJob::State::QUEUE, PageFetchJob::State::NEW)) {
                    job->Renew();
                }
            }
        }
        // Wait for WIP jobs to finish, then wipe everything
        fetch_thread_pool_.Barrier();
        for (auto &job : fetch_jobs_) {
            assert(job->GetState() == PageFetchJob::State::NEW ||
                   job->GetState() == PageFetchJob::State::DONE);
            job->Renew();
            job->ResetCursor();
        }
    }

    /**********************************************************************************************
     * Write() and helpers
     *********************************************************************************************/

    // On the first Write() we open a transaction with the outer db as a buffer for this and
    // subsequent writes, which we commit upon Sync() or Close(). We never explicitly rollback
    // this transaction, although rollback may occur if the process/host crashes. By Assumption
    // 2 above, inner db should always recover to a consistent state.
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

    // perform upsert operation in worker thread; ThreadPool runs these serially in enqueued
    // order
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
            PrefetchBarrier();
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
                    *outer_db_, "INSERT INTO " + inner_db_pages_table_ +
                                    "(data,meta1,meta2,pageno) VALUES(?,?,?,?)"));
            }
            if (!update_page_) {
                update_page_.reset(new SQLite::Statement(
                    *outer_db_, "UPDATE " + inner_db_pages_table_ +
                                    " SET data=?, meta1=?, meta2=? WHERE pageno=?"));
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
            PrefetchBarrier();
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
                delete_pages_.reset(new SQLite::Statement(
                    *outer_db_, "DELETE FROM " + inner_db_pages_table_ + " WHERE pageno > ?"));
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
        No-op: currently we hardcode locking_mode=EXCLUSIVE for the outer db, so there's no need
        to respond to inner db lock calls. This can be relaxed in the future by:
          - when we get Lock(SQLITE_LOCK_SHARED): open some read cursor on the outer db
          - when we get Lock(SQLITE_LOCK_EXCLUSIVE): BEGIN EXCLUSIVE txn on outer db
          - when we get xUnlock(SQLITE_LOCK_SHARED): Sync() if needed (only if inner db has
            PRAGMA synchronous=OFF)
          - when we get xUnlock(SQLITE_LOCK_NONE): close the read cursor we'd opened
        Holding the open cursor is supposed to ensure outer db lock only downgrades from
        EXCLUSIVE to SHARED upon txn commit, rather than releasing entirely. Testing of all this
        will be tricky, so we've punted on it for now.
        */
        return SQLITE_OK;
    }
    int Unlock(int eLock) override {
        if (eLock == SQLITE_LOCK_NONE) {
            // wipe internal state that's liable to go stale after relinquishing read lock
            page_count_ = 0;
            try {
                PrefetchBarrier();
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
        return SQLITE_IOCAP_ATOMIC | SQLITE_IOCAP_POWERSAFE_OVERWRITE;
        // We could optimize small write transactions by supporting SQLITE_IOCAP_BATCH_ATOMIC,
        // but there would be some tricky details to get just right, e.g. rollback commanded
        // through FileControl. We can easily support SQLITE_IOCAP_SEQUENTIAL and
        // SQLITE_IOCAP_SAFE_APPEND, but these don't help the main database file, only journals.
    }

    int Close() override {
        try {
            PrefetchBarrier();
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
        unsigned long long sequential = 0, non_sequential = 0;
        std::chrono::nanoseconds t_seek = std::chrono::nanoseconds::zero(),
                                 t_decode = std::chrono::nanoseconds::zero();
        for (const auto &job : fetch_jobs_) {
            sequential += job->sequential;
            non_sequential += job->non_sequential;
            t_seek += job->t_seek;
            t_decode += job->t_decode;
        }
        if (sequential + non_sequential) {
            _DBG << "reads sequential: " << sequential << " non-sequential: " << non_sequential
                 << " longest: " << longest_read_ << " prefetched: " << prefetch_wins_
                 << " wasted: " << prefetch_wasted_ << " seek: " << t_seek.count() / 1000000
                 << "ms decode: " << t_decode.count() / 1000000 << "ms" << _EOL;
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
        : outer_db_(std::move(outer_db)),
          inner_db_pages_table_(inner_db_tablename_prefix + "pages"), read_only_(read_only),
          // MAX(pageno) instead of COUNT(pageno) because the latter would trigger table scan
          select_page_count_(*outer_db_,
                             "SELECT IFNULL(MAX(pageno), 0) FROM " + inner_db_pages_table_),
          thread_pool_(threads, threads * 3), fetch_thread_pool_(threads, threads + 1),
          seek_interrupt_(false) {
        assert(threads);
        methods_.iVersion = 1;
        assert(outer_db_->execAndGet("PRAGMA quick_check").getString() == "ok");
        max_fetch_jobs_ = std::min((int)threads, MAX_FETCH_CURSORS);
        fetch_jobs_.reserve(MAX_FETCH_CURSORS); // important! ensure fetch_jobs_.data() never moves
    }
}; // namespace SQLiteNested

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

    // create page table; called within a transaction, subclasses may override and add aux
    // tables
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
                    // see comment in Lock() about possibe future relaxation of exclusive
                    // locking
                    outer_db->exec("PRAGMA locking_mode=EXCLUSIVE");
                    outer_db->exec("PRAGMA max_page_count=2147483646");
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
        // Remove SQLITE_OPEN_MAIN_JOURNAL to prevent SQLite os_unix.c from trying to copy owner
        // & perms of the inner database file, which doesn't exist from its POV. We assume
        // os_unix.c doesn't do anything else special for SQLITE_OPEN_MAIN_JOURNAL that might be
        // required for transaction safety! (The inner journal file is usually unused anyway.)
        // see: https://sqlite.org/src/file?name=src/os_unix.c&ci=tip
        // Note it has an assert that flags has -some- eType set; so, in case the host SQLite
        // has assertions on, we add SQLITE_OPEN_SUBJOURNAL when we remove
        // SQLITE_OPEN_MAIN_JOURNAL. The same issue will affect SQLITE_OPEN_WAL if/when we try
        // to suport WAL mode.
        if (flags & SQLITE_OPEN_MAIN_JOURNAL) {
            flags &= ~SQLITE_OPEN_MAIN_JOURNAL;
            flags |= SQLITE_OPEN_SUBJOURNAL;
        }
        assert(vfs_.szOsFile >= wrapped_->szOsFile);
        return wrapped_->xOpen(wrapped_, zName, pFile, flags, pOutFlags);
    }

    // Given user-provided db filename, use it as the outer db on the host filesystem, and
    // append a suffix as the inner db's filename (which won't actually exist on the host
    // filesystem, but xOpen() will recognize).
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
