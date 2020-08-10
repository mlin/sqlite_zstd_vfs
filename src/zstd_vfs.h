/**
 * Subclasses SQLiteNestedVFS to provide Zstandard page compression
 *
 * Once the database reaches a sufficient page count, we train a zstd dictionary to be used for
 * subsequent page compression ops, and store the dictionary in an aux table of the outer db for
 * later decompression use. Thereafter we create a new dictionary any time we've written enough
 * pages to have tripled the db size. Each dictionary is created from a random sample of 100
 * existing db pages, and has an autoincrement integer id. The meta1 column of the pages table
 * stores the id of the dict needed to decompress the page (-1 for no dict, and null if page is
 * stored uncompressed because it was not compressible).
 */
#pragma once

#include "SQLiteNestedVFS.h"
#include "zdict.h"
#include "zstd.h"
#include <map>
#include <random>
#include <set>
#include <sstream>

#define _EOL std::endl
#ifndef NDEBUG
#define _DBG std::cerr << __FILE__ << ":" << __LINE__ << ": "
#else
#define _DBG false && std::cerr
#endif

class ZstdInnerDatabaseFile : public SQLiteNested::InnerDatabaseFile {
  protected:
    using super = SQLiteNested::InnerDatabaseFile;
    std::default_random_engine R_;

    // false = disable use of dict for newly compressed pages
    bool enable_dict_;
    // zstd level for newly compressed pages
    int compression_level_;

    // how many inner db pages are sampled to train a new dict
    size_t dict_training_pages_;
    // targeted storage use for each dict (bytes)
    size_t dict_size_target_;

    // memoized SQL statements
    std::unique_ptr<SQLite::Statement> get_dict_, last_dict_id_, dict_pages_, put_dict_;
    using SQLiteNested::InnerDatabaseFile::StatementResetter;

    // zstd de/compression contexts
    std::shared_ptr<ZSTD_DCtx> dctx_;

    // in-memory cache of dict buffers and zstd de/compression dict structures, lazily populated
    struct dict_cache_entry {
        std::string dict;
        sqlite_int64 dict_page_count = 0;
        std::shared_ptr<ZSTD_CDict> cdict;
        std::shared_ptr<ZSTD_DDict> ddict;
        int level = 3;

        const ZSTD_CDict *ensure_cdict() {
            if (!cdict) {
                cdict = std::shared_ptr<ZSTD_CDict>(
                    ZSTD_createCDict(dict.data(), dict.size(), level), ZSTD_freeCDict);
                if (!cdict) {
                    throw SQLite::Exception("invalid zstd dictionary", SQLITE_CORRUPT);
                }
            }
            return cdict.get();
        }
        const ZSTD_DDict *ensure_ddict() {
            if (!ddict) {
                ddict = std::shared_ptr<ZSTD_DDict>(ZSTD_createDDict(dict.data(), dict.size()),
                                                    ZSTD_freeDDict);
                if (!ddict) {
                    throw SQLite::Exception("invalid zstd dictionary", SQLITE_CORRUPT);
                }
            }
            return ddict.get();
        }
        dict_cache_entry() = default;
        dict_cache_entry(const dict_cache_entry &) = delete;
        dict_cache_entry(dict_cache_entry &&) = default;
        dict_cache_entry &operator=(dict_cache_entry &&) = default;
    };
    std::map<sqlite_int64, dict_cache_entry> dict_cache_;

    dict_cache_entry &EnsureDictCached(sqlite_int64 dict_id) {
        assert(dict_id >= 0);
        auto existing = dict_cache_.find(dict_id);
        if (existing != dict_cache_.end()) {
            return existing->second;
        }
        if (!get_dict_) {
            get_dict_.reset(new SQLite::Statement(
                *outer_db_, "SELECT dict, page_count FROM nested_vfs_zstd_dicts WHERE id = ?"));
        }
        StatementResetter resetter(*get_dict_);
        get_dict_->bind(1, dict_id);
        if (!get_dict_->executeStep()) {
            throw SQLite::Exception("zstd page requires nonexistent dictionary", SQLITE_CORRUPT);
        }
        SQLite::Column dict = get_dict_->getColumn(0);
        assert(dict.isBlob());
        assert(get_dict_->getColumn(1).isInteger());

        dict_cache_entry ent;
        ent.dict.assign((const char *)dict.getBlob(), dict.getBytes());
        ent.dict_page_count = get_dict_->getColumn(1).getInt64();
        ent.level = compression_level_;
        dict_cache_[dict_id] = std::move(ent);
        return dict_cache_[dict_id];
    }

    struct ZstdPageFetchJob : public super::PageFetchJob {
        ZSTD_DCtx *dctx = nullptr;
        const ZSTD_DDict *ddict = nullptr;
        bool plain = false; // uncompressed page
        ZstdInnerDatabaseFile *that;

        ZstdPageFetchJob(ZstdInnerDatabaseFile &that_) : super::PageFetchJob(that_), that(&that_) {}

        ~ZstdPageFetchJob() {
            if (dctx) {
                ZSTD_freeDCtx(dctx);
            }
        }

        // After superclass seeks to a page, make sure we have the necessary decompression
        // dictionary ready for use. This has to be done in this serialized method since it may
        // need to load it from the outer db.
        void SeekCursor() override {
            super::PageFetchJob::SeekCursor();
            ddict = nullptr;
            plain = false;
            auto meta1 = cursor.getColumn(2);
            assert(std::string(meta1.getName()) == "meta1");
            if (meta1.isNull()) {
                plain = true;
            } else if (meta1.isInteger()) {
                sqlite3_int64 dict_id = meta1.getInt64();
                ddict = dict_id >= 0 ? that->EnsureDictCached(dict_id).ensure_ddict() : nullptr;
            } else {
                throw SQLite::Exception("unexpected meta1 entry in zstd page table",
                                        SQLITE_CORRUPT);
            }
        }

        // Perform decompression using dctx & ddict
        void DecodePage() override {
            SQLite::Column data = cursor.getColumn(1);
            assert(data.isBlob() && std::string(data.getName()) == "data");
            if (plain) { // uncompressed page
                return super::PageFetchJob::DecodePage();
            }
            if (!dctx) {
                dctx = ZSTD_createDCtx();
                if (!dctx) {
                    throw SQLite::Exception("ZSTD_createDCtx", SQLITE_NOMEM);
                }
            }
            size_t zrc;
            if (ddict) {
                zrc = ZSTD_decompress_usingDDict(dctx, EffectiveDest(), page_size, data.getBlob(),
                                                 data.getBytes(), ddict);
            } else {
                zrc = ZSTD_decompressDCtx(dctx, EffectiveDest(), page_size, data.getBlob(),
                                          data.getBytes());
            }
            if (zrc != page_size) {
                throw SQLite::Exception("zstd page decompression failed", SQLITE_CORRUPT);
            }
        }
    };

    std::unique_ptr<super::PageFetchJob> NewPageFetchJob() override {
        return std::unique_ptr<super::PageFetchJob>(new ZstdPageFetchJob(*this));
    }

    // dict currently used for compression of new pages
    sqlite_int64 cur_dict_ = -1;        // id (contents found in dict_cache_)
    size_t cur_dict_page_count_ = 0;    // page count at the time cur_dict was created
    size_t cur_dict_pages_written_ = 0; // # pages written since we last created a new dict

    // Take K samples without replacement from [0, N)
    // Floyd's algo adapted from https://stackoverflow.com/a/28287837/13393076
    std::vector<long long> sample_indices(size_t K, long long N) {
        std::set<long long> samples;
        for (long long t = N - K; t < N; t++) {
            auto i = std::uniform_int_distribution<long long>(0, t)(R_);
            assert(i < N);
            if (!samples.insert(i).second) {
                samples.insert(t);
            }
        }
        assert(samples.size() == K);
        return std::vector<long long>(samples.begin(), samples.end());
    }

    void UpdateCurDict() {
        if (!enable_dict_ || page_count_ < dict_training_pages_ ||
            page_count_ * page_size_ < 4 * dict_size_target_) {
            // db is too small relative to expected dict size
            return;
        }

        if (cur_dict_ < 0) {
            PrefetchBarrier();
            FinishUpserts();
            // start off with the newest dict stored in the database, if any
            if (!last_dict_id_) {
                last_dict_id_.reset(
                    new SQLite::Statement(*outer_db_, "SELECT MAX(id) FROM nested_vfs_zstd_dicts"));
            }
            StatementResetter resetter(*last_dict_id_);
            if (last_dict_id_->executeStep() && last_dict_id_->getColumn(0).isInteger()) {
                sqlite_int64 last_dict = last_dict_id_->getColumn(0).getInt64();
                if (last_dict >= 0) {
                    cur_dict_page_count_ = EnsureDictCached(last_dict).dict_page_count;
                    cur_dict_ = last_dict;
                    _DBG << "loaded dict " << cur_dict_ << " @ " << cur_dict_page_count_ << _EOL;
                }
            }
        }

        if (cur_dict_ < 0 ||
            std::max(page_count_, cur_dict_pages_written_) >= 3 * cur_dict_page_count_) {
            // time to create a new dict...
            PrefetchBarrier();
            FinishUpserts();
            auto t0 = std::chrono::high_resolution_clock::now();

            // read random pages
            auto page_indices = sample_indices(dict_training_pages_, page_count_);
            std::vector<char> pages(dict_training_pages_ * page_size_);
            for (int i = 0; i < dict_training_pages_; ++i) {
                // TODO: initiate prefetch of subsequent pages
                Read1Page(&pages[i * page_size_], page_indices[i]+1);
            }

            // build dict from the pages
            std::vector<size_t> pages_sizes(dict_training_pages_, page_size_);
            std::vector<char> dict(dict_size_target_);
            size_t zrc = ZDICT_trainFromBuffer(dict.data(), dict_size_target_, pages.data(),
                                               pages_sizes.data(), dict_training_pages_);
            if (ZDICT_isError(zrc)) {
                _DBG << "dict training failed: " << ZDICT_getErrorName(zrc) << _EOL;
                throw SQLite::Exception(ZDICT_getErrorName(zrc), SQLITE_IOERR_WRITE);
            }
            dict.resize(zrc);
            sqlite_int64 dict_page_count = std::max(cur_dict_pages_written_, page_count_);

            // put new dict into the db
            if (!put_dict_) {
                put_dict_.reset(new SQLite::Statement(
                    *outer_db_, "INSERT INTO nested_vfs_zstd_dicts(dict,page_count) VALUES(?,?)"));
            }
            StatementResetter put_dict_resetter(*put_dict_);
            put_dict_->bindNoCopy(1, dict.data(), dict.size());
            put_dict_->bind(2, dict_page_count);
            begin();
            if (put_dict_->exec() != 1) {
                throw SQLite::Exception("unexpected result from dict insert", SQLITE_IOERR_WRITE);
            }
            sqlite_int64 dict_id = outer_db_->getLastInsertRowid();
            if (dict_id < 0) {
                throw SQLite::Exception("unexpected rowid from dict insert", SQLITE_IOERR_WRITE);
            }
            EnsureDictCached(dict_id);

            // switch to the new dict
            cur_dict_ = dict_id;
            cur_dict_page_count_ = dict_page_count;
            cur_dict_pages_written_ = 0;
            std::chrono::nanoseconds t = std::chrono::high_resolution_clock::now() - t0;
            _DBG << "dict " << dict_id << " @ " << dict_page_count << " " << t.count() / 1000000
                 << "ms" << _EOL;
        }
    }

    struct CompressJob : super::EncodeJob {
        std::vector<char> buffer;
        ZSTD_CCtx *cctx = nullptr;
        sqlite3_int64 dict_id = -1;
        const ZSTD_CDict *cdict = nullptr;

        ~CompressJob() {
            if (cctx) {
                ZSTD_freeCCtx(cctx);
            }
        }
        void Execute() noexcept override {
            // Execute the job, possibly on a worker thread
            if (pageno > 1) {
                size_t capacity = ZSTD_compressBound(page.size());
                if (buffer.size() < capacity) {
                    buffer.resize(capacity);
                }
                size_t zrc;
                if (cdict) {
                    assert(dict_id >= 0);
                    zrc = ZSTD_compress_usingCDict(cctx, buffer.data(), buffer.size(), page.data(),
                                                   page.size(), cdict);
                    meta1 = dict_id;
                } else {
                    // no dict yet (first 100 pages); use fast setting (level -1)
                    zrc = ZSTD_compressCCtx(cctx, buffer.data(), buffer.size(), page.data(),
                                            page.size(), -1);
                    meta1 = -1;
                }
                if (!ZSTD_isError(zrc) && zrc * 10 < 8 * page.size()) {
                    assert(zrc <= buffer.size());
                    meta1null = false;
                    encoded_page = buffer.data();
                    encoded_page_size = zrc;
                    return;
                }
            }

            // fall through: call super to copy page uncompressed. one of:
            // - don't compress first page, as super looks at that to detect the db page size
            // - compression judged to have failed (ratio < 20%)
            assert(meta1null);
            super::EncodeJob::Execute();
        }
    };
    std::unique_ptr<super::EncodeJob> NewEncodeJob() override {
        return std::unique_ptr<super::EncodeJob>(new CompressJob);
    }
    void InitEncodeJob(super::EncodeJob &superjob, sqlite3_int64 pageno,
                       const void *page_data) override {
        super::InitEncodeJob(superjob, pageno, page_data);
        auto &job = reinterpret_cast<CompressJob &>(superjob);
        if (!job.cctx && !(job.cctx = ZSTD_createCCtx())) {
            throw SQLite::Exception("ZSTD_createCCtx", SQLITE_NOMEM);
        }
        UpdateCurDict();
        if (cur_dict_ >= 0) {
            job.dict_id = cur_dict_;
            job.cdict = EnsureDictCached(cur_dict_).ensure_cdict();
            cur_dict_pages_written_++;
        } else {
            job.dict_id = -1;
            job.cdict = nullptr;
        }
    }

  public:
    static const int DEFAULT_COMPRESSION_LEVEL = 3;
    static const size_t DEFAULT_DICT_TRAINING_PAGES = 100;
    static const size_t DEFAULT_DICT_SIZE_TARGET = 98304;
    ZstdInnerDatabaseFile(std::unique_ptr<SQLite::Database> &&outer_db,
                          const std::string &inner_db_tablename_prefix, bool read_only,
                          size_t threads, bool enable_dict = true,
                          int compression_level = DEFAULT_COMPRESSION_LEVEL,
                          size_t dict_training_pages = DEFAULT_DICT_TRAINING_PAGES,
                          size_t dict_size_target = DEFAULT_DICT_SIZE_TARGET)
        : SQLiteNested::InnerDatabaseFile(std::move(outer_db), inner_db_tablename_prefix, read_only,
                                          threads),
          enable_dict_(enable_dict), compression_level_(compression_level),
          dict_training_pages_(dict_training_pages), dict_size_target_(dict_size_target) {}
};

class ZstdVFS : public SQLiteNested::VFS {
  protected:
    void InitOuterDB(SQLite::Database &db) override {
        SQLiteNested::VFS::InitOuterDB(db);
        std::vector<const char *> ddl = {
            "CREATE TABLE nested_vfs_zstd_dicts (id INTEGER PRIMARY KEY AUTOINCREMENT, dict BLOB "
            "NOT NULL, page_count INTEGER NOT NULL)"};
        for (const auto &stmt : ddl) {
            SQLite::Statement(db, stmt).executeStep();
        }
    }

    virtual std::unique_ptr<SQLiteVFS::File>
    NewInnerDatabaseFile(const char *zName, std::unique_ptr<SQLite::Database> &&outer_db,
                         bool read_only, size_t threads) override {
        bool enable_dict = sqlite3_uri_boolean(zName, "dict", true);
        int compression_level =
            sqlite3_uri_int64(zName, "level", ZstdInnerDatabaseFile::DEFAULT_COMPRESSION_LEVEL);
        return std::unique_ptr<SQLiteVFS::File>(
            new ZstdInnerDatabaseFile(std::move(outer_db), inner_db_tablename_prefix_, read_only,
                                      threads, enable_dict, compression_level));
    }

  public:
    ZstdVFS() { inner_db_tablename_prefix_ = "nested_vfs_zstd_"; }
};

#undef _DBG
#undef _EOL
