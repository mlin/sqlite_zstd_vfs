#define CATCH_CONFIG_MAIN
#include "../src/SQLiteNestedVFS.h"
#include "SQLiteCpp/SQLiteCpp.h"
#include "catch2/catch.hpp"
#include "sqlite3.h"
#include <iostream>
#include <vector>

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
        experiment.exec("PRAGMA journal_mode=MEMORY; PRAGMA auto_vacuum=FULL");
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
