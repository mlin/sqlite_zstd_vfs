#define CATCH_CONFIG_MAIN
#define SQLITE_CORE
#include "SQLiteCpp/SQLiteCpp.h"
#include "catch2/catch.hpp"
#include <iostream>
#include <sqlite3ext.h>
#include <vector>

#include "../src/SQLiteNestedVFS.h"

using namespace std;

extern "C" int vfstrace_register(const char *, const char *, int (*xOut)(const char *, FILE *),
                                 void *, int);

void setup() {
    static bool first = true;
    if (first) {
        (new SQLiteNested::VFS)->Register("nested");
        vfstrace_register("nested_trace", "nested", fputs, stderr, 0);
        first = false;
    }
}

TEST_CASE("hello") {
    string testdir("/tmp/sqlite3_nested_vfs_test_XXXXXX");
    REQUIRE(mkdtemp((char *)testdir.data()) != nullptr);

    setup();

    {
        SQLite::Database guest(testdir + "/inception", SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE,
                               0, "nested_trace");

        SQLite::Transaction transaction(guest);

        guest.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)");

        int nb = guest.exec("INSERT INTO test VALUES (NULL, \"test\")");
        std::cerr << "INSERT INTO test VALUES (NULL, \"test\")\", returned " << nb << std::endl;

        // Commit transaction
        transaction.commit();
    }

    {
        SQLite::Database guest(testdir + "/inception", SQLite::OPEN_READONLY, 0, "nested_trace");
        SQLite::Statement q(guest, "SELECT * FROM test");
        while (q.executeStep()) {
            std::cerr << q.getColumn(1).getString() << std::endl;
        }
    }
}

TEST_CASE("bigpages") {
    string testdir("/tmp/sqlite3_nested_vfs_test_XXXXXX");
    REQUIRE(mkdtemp((char *)testdir.data()) != nullptr);

    setup();

    {
        SQLite::Database guest("file:" + testdir + "/inception?outer_unsafe",
                               SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE | SQLite::OPEN_URI, 0,
                               "nested_trace");
        guest.exec(SQLiteNested::UNSAFE_PRAGMAS);
        guest.exec("PRAGMA page_size=16384");

        SQLite::Transaction transaction(guest);

        guest.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)");

        int nb = guest.exec("INSERT INTO test VALUES (NULL, \"test\")");
        std::cerr << "INSERT INTO test VALUES (NULL, \"test\")\", returned " << nb << std::endl;

        // Commit transaction
        transaction.commit();

        guest.exec("INSERT INTO test VALUES(NULL, 'foo')");

        SQLite::Statement q(guest, "SELECT * FROM test");
        while (q.executeStep()) {
            std::cerr << q.getColumn(1).getString() << std::endl;
        }
    }
}

void ElementaryCellularAutomaton(uint8_t rule, const vector<bool> &state, vector<bool> &ans) {
    size_t size2 = state.size() + 2;
    ans.clear();
    ans.reserve(size2);
    for (size_t i = 0; i < size2; i++) {
        int which_bit = 0;
        if (i > 1 && state[i - 2]) {
            which_bit += 4;
        }
        if (i > 0 && i <= state.size() && state[i - 1]) {
            which_bit += 2;
        }
        if (i < state.size() && state[i]) {
            which_bit++;
        }
        ans.push_back((rule >> which_bit) & 1);
    }
}

TEST_CASE("blob I/O") {
    string testdir("/tmp/sqlite3_nested_vfs_test_XXXXXX");
    REQUIRE(mkdtemp((char *)testdir.data()) != nullptr);

    setup();

    vector<uint8_t> blob;
    vector<vector<bool>> state = {{true}, {}};
    for (int i = 0; i < 4096; ++i) {
        if (i && i % 4 == 0) {
            for (int j = 0; j < state[0].size() / 8; ++j) {
                uint8_t byte = 0;
                for (int k = 0; k < 8; ++k) {
                    byte <<= 1;
                    byte |= state[0][8 * j + k] ? 1 : 0;
                }
                blob.push_back(byte);
            }
        }
        ElementaryCellularAutomaton(30, state[i % 2], state[(i + 1) % 2]);
    }

    {
        SQLite::Database guest("file:" + testdir + "/inception?outer_unsafe",
                               SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE | SQLite::OPEN_URI, 0,
                               "nested_trace");
        guest.exec(SQLiteNested::UNSAFE_PRAGMAS);
        guest.exec("PRAGMA page_size=16384");

        SQLite::Transaction transaction(guest);

        guest.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value BLOB)");

        SQLite::Statement ins(guest, "INSERT INTO test(value) VALUES(?)");
        ins.bindNoCopy(1, blob.data(), blob.size());
        ins.exec();
        ins.reset();
        ins.exec();
        transaction.commit();
    }

    {
        SQLite::Database dbh(testdir + "/inception", SQLite::OPEN_READWRITE, 0, "nested_trace");

        sqlite3_blob *h = nullptr;
        REQUIRE(sqlite3_blob_open(dbh.getHandle(), "main", "test", "value", 2, 1, &h) == SQLITE_OK);
        REQUIRE(sqlite3_blob_bytes(h) == blob.size());

        const int N = 65536, ofs = 42424;
        REQUIRE(ofs + N < blob.size());
        vector<uint8_t> buf(N, 0);

        REQUIRE(sqlite3_blob_read(h, buf.data(), N, ofs) == SQLITE_OK);
        REQUIRE(memcmp(buf.data(), &blob[ofs], N) == 0);

        for (int i = 0; i < N; ++i) {
            buf[i] = ~buf[i];
        }

        SQLite::Transaction txn(dbh);
        REQUIRE(sqlite3_blob_write(h, buf.data(), N, ofs) == SQLITE_OK);
        REQUIRE(sqlite3_blob_close(h) == SQLITE_OK);
        h = nullptr;
        txn.commit();

        REQUIRE(sqlite3_blob_open(dbh.getHandle(), "main", "test", "value", 2, 1, &h) == SQLITE_OK);
        vector<uint8_t> buf2(N, 0);
        REQUIRE(sqlite3_blob_read(h, buf2.data(), N, ofs) == SQLITE_OK);
        REQUIRE(memcmp(buf.data(), buf2.data(), N) == 0);
        REQUIRE(sqlite3_blob_close(h) == SQLITE_OK);
    }
}

bool UpdateCells(SQLite::Database &db, SQLite::Statement &get_state, SQLite::Statement &insert,
                 SQLite::Statement &update, uint8_t rule, int log_rule = -1) {
    SQLite::Transaction txn(db);
    vector<bool> state, new_state;
    sqlite3_int64 n = 0, last_pos;
    for (get_state.bind(1, rule); get_state.executeStep(); ++n) {
        state.push_back(get_state.getColumn(0).getInt64() != 0);
        last_pos = get_state.getColumn(1).getInt64();
    }
    get_state.reset();
    assert(last_pos == n / 2);
    if (rule == log_rule) {
        for (int i = n / 2; i < std::min(state.size(), (size_t)(n / 2 + 100)); i++) {
            cerr << (state[i] ? "█" : " ");
        }
        cerr << endl;
    }
    ElementaryCellularAutomaton(rule, state, new_state);
    bool ans;
    for (sqlite3_int64 i = 0; i < new_state.size(); ++i) {
        auto pos = i - sqlite3_int64(new_state.size()) / 2;
        auto *stmt = (pos >= (0 - n / 2) && pos <= n / 2) ? &update : &insert;
        stmt->bind(1, new_state[i] ? 1 : 0);
        stmt->bind(2, rule);
        stmt->bind(3, pos);
        stmt->exec();
        stmt->reset();
        if (pos == 0) {
            ans = new_state[i];
        }
    }
    txn.commit();
    return ans;
}

TEST_CASE("cellular_automata") {
    string testdir("/tmp/sqlite3_inception_vfs_test_XXXXXX");
    REQUIRE(mkdtemp((char *)testdir.data()) != nullptr);

    setup();

    {
        SQLite::Database control(testdir + "/control",
                                 SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE);
        SQLite::Database experiment("file:" + testdir + "/experiment?vfs=nested&threads=-1",
                                    SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE |
                                        SQLite::OPEN_URI);

        auto schema = "CREATE TABLE cellular_automata(state INTEGER, rule INTEGER, pos INTEGER, "
                      "PRIMARY KEY(rule,pos));"
                      "CREATE INDEX dummy_index ON cellular_automata(state)";
        control.exec(schema);
        experiment.exec(
            "PRAGMA journal_mode=MEMORY; PRAGMA auto_vacuum=FULL; PRAGMA cache_size=-64");
        experiment.exec(schema);

        auto insert = "INSERT INTO cellular_automata(state,rule,pos) VALUES(?,?,?)",
             update = "UPDATE cellular_automata SET state = ? WHERE rule = ? and pos = ?";

        SQLite::Statement ctrl_insert(control, insert), ctrl_update(control, update),
            expt_insert(experiment, insert), expt_update(experiment, update);

        vector<uint8_t> rules = {30,  54,  60,  62,  90,  94,  102, 110, 122,
                                 126, 150, 158, 182, 188, 190, 220, 222, 250};
        {
            SQLite::Transaction ctrl_txn(control);
            SQLite::Transaction expt_txn(experiment);
            for (auto rule : rules) {
                for (auto *stmt : {&ctrl_insert, &expt_insert}) {
                    stmt->bind(1, 1);
                    stmt->bind(2, rule);
                    stmt->bind(3, 0);
                    stmt->exec();
                    stmt->reset();
                }
            }
            ctrl_txn.commit();
            expt_txn.commit();
        }

        auto get_state = "SELECT state, pos FROM cellular_automata WHERE rule = ? ORDER BY pos";
        SQLite::Statement ctrl_get_state(control, get_state), expt_get_state(experiment, get_state);
        cerr << endl;
        for (int t = 1; t <= 250; t++) {
            bool rule30bit;
            for (auto rule : rules) {
                bool ans = UpdateCells(control, ctrl_get_state, ctrl_insert, ctrl_update, rule, 30);
                rule30bit = rule == 30 ? ans : rule30bit;
                UpdateCells(experiment, expt_get_state, expt_insert, expt_update, rule);
            }
            // just to make it a bit more interesting -- permute the order in which we run the
            // updates, with a small dose of entropy from rule 30
            auto cut_pos = 1 + ((t * rules[rule30bit ? 1 : 0] / 2) % (rules.size() - 2));
            std::vector<uint8_t> new_rules(rules.begin() + cut_pos, rules.end());
            new_rules.insert(new_rules.end(), rules.begin(), rules.begin() + cut_pos);
            REQUIRE(new_rules.size() == rules.size());
        }
    }
    {
        SQLite::Database control(testdir + "/control", SQLite::OPEN_READONLY);
        SQLite::Database experiment("file:" + testdir + "/experiment?vfs=nested",
                                    SQLite::OPEN_READONLY | SQLite::OPEN_URI);
        // verify db identity
        auto get_all = "SELECT state, rule, pos FROM cellular_automata ORDER BY rule, pos";
        SQLite::Statement ctrl_get_all(control, get_all), expt_get_all(experiment, get_all);
        while (ctrl_get_all.executeStep()) {
            REQUIRE(expt_get_all.executeStep());
            REQUIRE(ctrl_get_all.getColumn(0).getInt64() == expt_get_all.getColumn(0).getInt64());
            REQUIRE(ctrl_get_all.getColumn(1).getInt64() == expt_get_all.getColumn(1).getInt64());
            REQUIRE(ctrl_get_all.getColumn(2).getInt64() == expt_get_all.getColumn(2).getInt64());
        }
        REQUIRE(!expt_get_all.executeStep());
        ctrl_get_all.reset();
        expt_get_all.reset();
        experiment.exec("VACUUM INTO 'file:" + testdir + "/experiment_vacuumed?vfs=nested'");
    }
    system(("ls -lh " + testdir).c_str());
}

/*
The following BS is needed for this test executable to use SQLiteCpp even though it's built for use
within a loadable extension (SQLITECPP_IN_EXTENSION). The sqlite3Apis definition is copied from
    https://github.com/sqlite/sqlite/blob/master/src/loadext.c
and must match the build environment's SQLite version. Take care to keep the small declaration
following the sqlite3Apis definition.
*/
static const sqlite3_api_routines sqlite3Apis = {
    sqlite3_aggregate_context,
#ifndef SQLITE_OMIT_DEPRECATED
    sqlite3_aggregate_count,
#else
    0,
#endif
    sqlite3_bind_blob,
    sqlite3_bind_double,
    sqlite3_bind_int,
    sqlite3_bind_int64,
    sqlite3_bind_null,
    sqlite3_bind_parameter_count,
    sqlite3_bind_parameter_index,
    sqlite3_bind_parameter_name,
    sqlite3_bind_text,
    sqlite3_bind_text16,
    sqlite3_bind_value,
    sqlite3_busy_handler,
    sqlite3_busy_timeout,
    sqlite3_changes,
    sqlite3_close,
    sqlite3_collation_needed,
    sqlite3_collation_needed16,
    sqlite3_column_blob,
    sqlite3_column_bytes,
    sqlite3_column_bytes16,
    sqlite3_column_count,
    sqlite3_column_database_name,
    sqlite3_column_database_name16,
    sqlite3_column_decltype,
    sqlite3_column_decltype16,
    sqlite3_column_double,
    sqlite3_column_int,
    sqlite3_column_int64,
    sqlite3_column_name,
    sqlite3_column_name16,
    sqlite3_column_origin_name,
    sqlite3_column_origin_name16,
    sqlite3_column_table_name,
    sqlite3_column_table_name16,
    sqlite3_column_text,
    sqlite3_column_text16,
    sqlite3_column_type,
    sqlite3_column_value,
    sqlite3_commit_hook,
    sqlite3_complete,
    sqlite3_complete16,
    sqlite3_create_collation,
    sqlite3_create_collation16,
    sqlite3_create_function,
    sqlite3_create_function16,
    sqlite3_create_module,
    sqlite3_data_count,
    sqlite3_db_handle,
    sqlite3_declare_vtab,
    sqlite3_enable_shared_cache,
    sqlite3_errcode,
    sqlite3_errmsg,
    sqlite3_errmsg16,
    sqlite3_exec,
#ifndef SQLITE_OMIT_DEPRECATED
    sqlite3_expired,
#else
    0,
#endif
    sqlite3_finalize,
    sqlite3_free,
    sqlite3_free_table,
    sqlite3_get_autocommit,
    sqlite3_get_auxdata,
    sqlite3_get_table,
    0, /* Was sqlite3_global_recover(), but that function is deprecated */
    sqlite3_interrupt,
    sqlite3_last_insert_rowid,
    sqlite3_libversion,
    sqlite3_libversion_number,
    sqlite3_malloc,
    sqlite3_mprintf,
    sqlite3_open,
    sqlite3_open16,
    sqlite3_prepare,
    sqlite3_prepare16,
    sqlite3_profile,
    sqlite3_progress_handler,
    sqlite3_realloc,
    sqlite3_reset,
    sqlite3_result_blob,
    sqlite3_result_double,
    sqlite3_result_error,
    sqlite3_result_error16,
    sqlite3_result_int,
    sqlite3_result_int64,
    sqlite3_result_null,
    sqlite3_result_text,
    sqlite3_result_text16,
    sqlite3_result_text16be,
    sqlite3_result_text16le,
    sqlite3_result_value,
    sqlite3_rollback_hook,
    sqlite3_set_authorizer,
    sqlite3_set_auxdata,
    sqlite3_snprintf,
    sqlite3_step,
    sqlite3_table_column_metadata,
#ifndef SQLITE_OMIT_DEPRECATED
    sqlite3_thread_cleanup,
#else
    0,
#endif
    sqlite3_total_changes,
    sqlite3_trace,
#ifndef SQLITE_OMIT_DEPRECATED
    sqlite3_transfer_bindings,
#else
    0,
#endif
    sqlite3_update_hook,
    sqlite3_user_data,
    sqlite3_value_blob,
    sqlite3_value_bytes,
    sqlite3_value_bytes16,
    sqlite3_value_double,
    sqlite3_value_int,
    sqlite3_value_int64,
    sqlite3_value_numeric_type,
    sqlite3_value_text,
    sqlite3_value_text16,
    sqlite3_value_text16be,
    sqlite3_value_text16le,
    sqlite3_value_type,
    sqlite3_vmprintf,
    /*
    ** The original API set ends here.  All extensions can call any
    ** of the APIs above provided that the pointer is not NULL.  But
    ** before calling APIs that follow, extension should check the
    ** sqlite3_libversion_number() to make sure they are dealing with
    ** a library that is new enough to support that API.
    *************************************************************************
    */
    sqlite3_overload_function,

    /*
    ** Added after 3.3.13
    */
    sqlite3_prepare_v2,
    sqlite3_prepare16_v2,
    sqlite3_clear_bindings,

    /*
    ** Added for 3.4.1
    */
    sqlite3_create_module_v2,

    /*
    ** Added for 3.5.0
    */
    sqlite3_bind_zeroblob,
    sqlite3_blob_bytes,
    sqlite3_blob_close,
    sqlite3_blob_open,
    sqlite3_blob_read,
    sqlite3_blob_write,
    sqlite3_create_collation_v2,
    sqlite3_file_control,
    sqlite3_memory_highwater,
    sqlite3_memory_used,
#ifdef SQLITE_MUTEX_OMIT
    0,
    0,
    0,
    0,
    0,
#else
    sqlite3_mutex_alloc,
    sqlite3_mutex_enter,
    sqlite3_mutex_free,
    sqlite3_mutex_leave,
    sqlite3_mutex_try,
#endif
    sqlite3_open_v2,
    sqlite3_release_memory,
    sqlite3_result_error_nomem,
    sqlite3_result_error_toobig,
    sqlite3_sleep,
    sqlite3_soft_heap_limit,
    sqlite3_vfs_find,
    sqlite3_vfs_register,
    sqlite3_vfs_unregister,

    /*
    ** Added for 3.5.8
    */
    sqlite3_threadsafe,
    sqlite3_result_zeroblob,
    sqlite3_result_error_code,
    sqlite3_test_control,
    sqlite3_randomness,
    sqlite3_context_db_handle,

    /*
    ** Added for 3.6.0
    */
    sqlite3_extended_result_codes,
    sqlite3_limit,
    sqlite3_next_stmt,
    sqlite3_sql,
    sqlite3_status,

    /*
    ** Added for 3.7.4
    */
    sqlite3_backup_finish,
    sqlite3_backup_init,
    sqlite3_backup_pagecount,
    sqlite3_backup_remaining,
    sqlite3_backup_step,
#ifndef SQLITE_OMIT_COMPILEOPTION_DIAGS
    sqlite3_compileoption_get,
    sqlite3_compileoption_used,
#else
    0,
    0,
#endif
    sqlite3_create_function_v2,
    sqlite3_db_config,
    sqlite3_db_mutex,
    sqlite3_db_status,
    sqlite3_extended_errcode,
    sqlite3_log,
    sqlite3_soft_heap_limit64,
    sqlite3_sourceid,
    sqlite3_stmt_status,
    sqlite3_strnicmp,
#ifdef SQLITE_ENABLE_UNLOCK_NOTIFY
    sqlite3_unlock_notify,
#else
    0,
#endif
#ifndef SQLITE_OMIT_WAL
    sqlite3_wal_autocheckpoint,
    sqlite3_wal_checkpoint,
    sqlite3_wal_hook,
#else
    0,
    0,
    0,
#endif
    sqlite3_blob_reopen,
    sqlite3_vtab_config,
    sqlite3_vtab_on_conflict,
    sqlite3_close_v2,
    sqlite3_db_filename,
    sqlite3_db_readonly,
    sqlite3_db_release_memory,
    sqlite3_errstr,
    sqlite3_stmt_busy,
    sqlite3_stmt_readonly,
    sqlite3_stricmp,
    sqlite3_uri_boolean,
    sqlite3_uri_int64,
    sqlite3_uri_parameter,
    sqlite3_vsnprintf,
    sqlite3_wal_checkpoint_v2,
    /* Version 3.8.7 and later */
    sqlite3_auto_extension,
    sqlite3_bind_blob64,
    sqlite3_bind_text64,
    sqlite3_cancel_auto_extension,
    sqlite3_load_extension,
    sqlite3_malloc64,
    sqlite3_msize,
    sqlite3_realloc64,
    sqlite3_reset_auto_extension,
    sqlite3_result_blob64,
    sqlite3_result_text64,
    sqlite3_strglob,
    /* Version 3.8.11 and later */
    (sqlite3_value *(*)(const sqlite3_value *))sqlite3_value_dup,
    sqlite3_value_free,
    sqlite3_result_zeroblob64,
    sqlite3_bind_zeroblob64,
    /* Version 3.9.0 and later */
    sqlite3_value_subtype,
    sqlite3_result_subtype,
    /* Version 3.10.0 and later */
    sqlite3_status64,
    sqlite3_strlike,
    sqlite3_db_cacheflush,
    /* Version 3.12.0 and later */
    sqlite3_system_errno,
    /* Version 3.14.0 and later */
    sqlite3_trace_v2,
    sqlite3_expanded_sql,
    /* Version 3.18.0 and later */
    sqlite3_set_last_insert_rowid,
    /* Version 3.20.0 and later */
    sqlite3_prepare_v3,
    sqlite3_prepare16_v3,
    sqlite3_bind_pointer,
    sqlite3_result_pointer,
    sqlite3_value_pointer,
    /* Version 3.22.0 and later */
    sqlite3_vtab_nochange,
    sqlite3_value_nochange,
    sqlite3_vtab_collation,
    /* Version 3.24.0 and later */
    sqlite3_keyword_count,
    sqlite3_keyword_name,
    sqlite3_keyword_check,
    sqlite3_str_new,
    sqlite3_str_finish,
    sqlite3_str_appendf,
    sqlite3_str_vappendf,
    sqlite3_str_append,
    sqlite3_str_appendall,
    sqlite3_str_appendchar,
    sqlite3_str_reset,
    sqlite3_str_errcode,
    sqlite3_str_length,
    sqlite3_str_value,
    /* Version 3.25.0 and later */
    sqlite3_create_window_function,
/* Version 3.26.0 and later */
#ifdef SQLITE_ENABLE_NORMALIZE
    sqlite3_normalized_sql,
#else
    0,
#endif
    /* Version 3.28.0 and later */
    sqlite3_stmt_isexplain,
    sqlite3_value_frombind,
/* Version 3.30.0 and later */
#ifndef SQLITE_OMIT_VIRTUALTABLE
    sqlite3_drop_modules,
#else
    0,
#endif
    /* Version 3.31.0 and later */
    sqlite3_hard_heap_limit64,
    sqlite3_uri_key,
    sqlite3_filename_database,
    sqlite3_filename_journal,
    sqlite3_filename_wal,
};

extern "C" {
const sqlite3_api_routines *sqlite3_api = &sqlite3Apis;
}
