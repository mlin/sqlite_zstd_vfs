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
    std::shared_ptr<ZSTD_CCtx> cctx_;

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
            // time to create a new dict

            sqlite_int64 i;
            // fetch random pages
            if (!dict_pages_) {
                std::ostringstream sql;
                sql << "SELECT data, meta1, meta2 FROM nested_vfs_zstd_pages WHERE page_idx IN (?";
                for (i = 1; i < dict_training_pages_; i++) {
                    sql << ",?";
                }
                sql << ")";
                dict_pages_.reset(new SQLite::Statement(*outer_db_, sql.str()));
            }
            auto page_indices = sample_indices(dict_training_pages_, page_count_);
            std::vector<char> pages(dict_training_pages_ * page_size_);
            StatementResetter dict_pages_resetter(*dict_pages_);
            for (i = 0; i < dict_training_pages_; i++) {
                dict_pages_->bind(i + 1, page_indices[i]);
            }
            for (i = 0; i < dict_training_pages_ && dict_pages_->executeStep(); i++) {
                DecodePage(page_indices[i], dict_pages_->getColumn(0), dict_pages_->getColumn(1),
                           dict_pages_->getColumn(2), pages.data() + (i * page_size_));
            }
            if (i != dict_training_pages_ || dict_pages_->executeStep()) {
                throw SQLite::Exception("missing/malformed pages in zstd VFS database",
                                        SQLITE_CORRUPT);
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
            commit();
            SQLite::Transaction txn(*outer_db_);
            StatementResetter put_dict_resetter(*put_dict_);
            put_dict_->bindNoCopy(1, dict.data(), dict.size());
            put_dict_->bind(2, dict_page_count);
            if (put_dict_->exec() != 1) {
                throw SQLite::Exception("unexpected result from dict insert", SQLITE_IOERR_WRITE);
            }
            sqlite_int64 dict_id = outer_db_->getLastInsertRowid();
            if (dict_id < 0) {
                throw SQLite::Exception("unexpected rowid from dict insert", SQLITE_IOERR_WRITE);
            }
            EnsureDictCached(dict_id);
            txn.commit();

            // switch to the new dict
            cur_dict_ = dict_id;
            cur_dict_page_count_ = dict_page_count;
            cur_dict_pages_written_ = 0;
            _DBG << "dict " << dict_id << " @ " << dict_page_count << _EOL;
        }
    }

    // Decode one page from the blob stored in the outer db (page_size_ bytes out)
    virtual void DecodePage(sqlite_int64 page_idx, const SQLite::Column &data,
                            const SQLite::Column &meta1, const SQLite::Column &meta2,
                            void *dest) override {
        if (meta1.isInteger()) {
            if (!dctx_) {
                dctx_ = std::shared_ptr<ZSTD_DCtx>(ZSTD_createDCtx(), ZSTD_freeDCtx);
                if (!dctx_) {
                    throw SQLite::Exception("ZSTD_createDCtx", SQLITE_NOMEM);
                }
            }
            sqlite_int64 dict_id = meta1.getInt64();
            size_t zrc;
            if (dict_id >= 0) {
                zrc = ZSTD_decompress_usingDDict(dctx_.get(), dest, page_size_, data.getBlob(),
                                                 data.getBytes(),
                                                 EnsureDictCached(dict_id).ensure_ddict());
            } else {
                zrc = ZSTD_decompressDCtx(dctx_.get(), dest, page_size_, data.getBlob(),
                                          data.getBytes());
            }
            if (zrc != page_size_) {
                throw SQLite::Exception("zstd page decompression failed", SQLITE_CORRUPT);
            }
        } else if (meta1.isNull()) { // uncompressed page
            SQLiteNested::InnerDatabaseFile::DecodePage(page_idx, data, meta1, meta2, dest);
        } else {
            throw SQLite::Exception("unexpected meta1 entry in zstd page table", SQLITE_CORRUPT);
        }
    }

    // Encode one page for storage as a blob in the outer db (page_size_ bytes in)
    // Bind columns: 1=data, 2=meta1, 3=meta2
    // pagebuf is scratch space, e.g. for use with upsert.bindNoCopy()
    virtual void EncodePage(sqlite_int64 page_idx, const void *src, std::vector<char> &pagebuf,
                            SQLite::Statement &upsert) override {
        if (page_idx > 0) {
            if (!cctx_) {
                cctx_ = std::shared_ptr<ZSTD_CCtx>(ZSTD_createCCtx(), ZSTD_freeCCtx);
                if (!cctx_) {
                    throw SQLite::Exception("ZSTD_createCCtx", SQLITE_NOMEM);
                }
            }
            UpdateCurDict();

            size_t capacity = ZSTD_compressBound(page_size_);
            pagebuf.resize(capacity);
            size_t zrc;
            if (enable_dict_ && cur_dict_ >= 0) {
                zrc = ZSTD_compress_usingCDict(cctx_.get(), pagebuf.data(), capacity, src,
                                               page_size_, dict_cache_[cur_dict_].ensure_cdict());
                upsert.bind(2, cur_dict_);
                cur_dict_pages_written_++;
            } else {
                // no dict yet; use fast setting (level -1) for first 100 pages
                zrc = ZSTD_compressCCtx(cctx_.get(), pagebuf.data(), capacity, src, page_size_,
                                        page_idx > DEFAULT_DICT_TRAINING_PAGES ? compression_level_
                                                                               : -1);
                upsert.bind(2, (sqlite3_int64)-1);
            }
            if (!ZSTD_isError(zrc) && zrc * 10 < 8 * page_size_) {
                assert(zrc <= capacity);
                upsert.bindNoCopy(1, pagebuf.data(), zrc);
                return;
            }
        }

        // fall through: call super to copy page uncompressed. one of:
        // - don't compress page_idx=0, as super looks at that to detect the db page size
        // - compression judged to have failed (ratio < 20%)
        upsert.bind(2); // meta1=NULL
        SQLiteNested::InnerDatabaseFile::EncodePage(page_idx, src, pagebuf, upsert);
    }

  public:
    static const int DEFAULT_COMPRESSION_LEVEL = 3;
    static const size_t DEFAULT_DICT_TRAINING_PAGES = 100;
    static const size_t DEFAULT_DICT_SIZE_TARGET = 98304;
    ZstdInnerDatabaseFile(std::unique_ptr<SQLite::Database> &&outer_db,
                          const std::string &inner_db_tablename_prefix, bool read_only,
                          bool enable_dict = true,
                          int compression_level = DEFAULT_COMPRESSION_LEVEL,
                          size_t dict_training_pages = DEFAULT_DICT_TRAINING_PAGES,
                          size_t dict_size_target = DEFAULT_DICT_SIZE_TARGET)
        : SQLiteNested::InnerDatabaseFile(std::move(outer_db), inner_db_tablename_prefix,
                                          read_only),
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
                         bool read_only) override {
        bool enable_dict = sqlite3_uri_boolean(zName, "dict", true);
        int compression_level =
            sqlite3_uri_int64(zName, "level", ZstdInnerDatabaseFile::DEFAULT_COMPRESSION_LEVEL);
        return std::unique_ptr<SQLiteVFS::File>(
            new ZstdInnerDatabaseFile(std::move(outer_db), inner_db_tablename_prefix_, read_only,
                                      enable_dict, compression_level));
    }

  public:
    ZstdVFS() { inner_db_tablename_prefix_ = "nested_vfs_zstd_"; }
};

#undef _DBG
#undef _EOL
