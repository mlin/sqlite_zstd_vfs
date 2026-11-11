import contextlib
import json
import multiprocessing
import os
import shlex
import sqlite3
import subprocess
import sys
import time
import urllib.parse

import pytest

HERE = os.path.dirname(__file__)
BUILD = os.path.abspath(os.path.join(HERE, "..", "build"))


@pytest.fixture(scope="session")
def chinook_file(tmpdir_factory):
    dn = tmpdir_factory.mktemp("chinook")
    subprocess.run(
        [
            "wget",
            "-nv",
            "https://github.com/lerocha/chinook-database/raw/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite",
        ],
        check=True,
        cwd=dn,
    )
    return str(dn.join("Chinook_Sqlite.sqlite"))


def test_roundtrip(tmpdir, chinook_file):
    # read the SQL dump
    rslt = subprocess.run(
        f"sqlite3 :memory: -bail -cmd '.open file:{chinook_file}?mode=ro' -cmd .dump -cmd .exit",
        check=True,
        shell=True,
        cwd=tmpdir,
        stdout=subprocess.PIPE,
        text=True,
    )
    expected = [stmt.strip() for stmt in rslt.stdout.split(";\n")]

    # open the nested database
    con = sqlite3.connect(":memory:")
    con.enable_load_extension(True)
    con.load_extension(os.path.join(BUILD, "nested_vfs"))
    dbfn = os.path.join(tmpdir, "chinook_nested.sqlite")
    con = sqlite3.connect(f"file:{dbfn}?vfs=nested", uri=True)

    # replay the sql
    for stmt in expected:
        con.execute(stmt)

    # run a query
    n = 0
    for row in con.execute(
        """
    select e.*, count(i.invoiceid) as 'Total Number of Sales'
    from employee as e
        join customer as c on e.employeeid = c.supportrepid
        join invoice as i on i.customerid = c.customerid
    group by e.employeeid
    """
    ):
        n += 1
    assert n == 3
    con.close()

    # dump from the nested version
    rslt = subprocess.run(
        f"sqlite3 :memory: -bail -cmd '.load {os.path.join(BUILD, 'nested_vfs')}' -cmd '.open file:{dbfn}?mode=ro&vfs=nested' -cmd .dump -cmd .exit",
        check=True,
        shell=True,
        cwd=tmpdir,
        stdout=subprocess.PIPE,
        text=True,
    )
    actual = [stmt.strip() for stmt in rslt.stdout.split(";\n")]
    assert actual == expected


def test_roundtrip_zstd(tmpdir, chinook_file):
    # read the SQL dump
    rslt = subprocess.run(
        f"sqlite3 :memory: -bail -cmd '.open file:{chinook_file}?mode=ro' -cmd .dump -cmd .exit",
        check=True,
        shell=True,
        cwd=tmpdir,
        stdout=subprocess.PIPE,
        text=True,
    )
    expected = [stmt.strip() for stmt in rslt.stdout.split(";\n")]

    # open the nested database
    con = sqlite3.connect(":memory:")
    con.enable_load_extension(True)
    con.load_extension(os.path.join(BUILD, "zstd_vfs"))
    dbfn = os.path.join(tmpdir, "chinook_nested.sqlite")
    con = sqlite3.connect(f"file:{dbfn}?vfs=zstd", uri=True)

    # replay the sql
    for stmt in expected:
        con.execute(stmt)

    # run a query
    n = 0
    for row in con.execute(
        """
    select e.*, count(i.invoiceid) as 'Total Number of Sales'
    from employee as e
        join customer as c on e.employeeid = c.supportrepid
        join invoice as i on i.customerid = c.customerid
    group by e.employeeid
    """
    ):
        n += 1
    assert n == 3
    con.close()

    # dump from the zstd version
    rslt = subprocess.run(
        f"sqlite3 :memory: -bail -cmd '.load {os.path.join(BUILD, 'zstd_vfs')}' -cmd '.open file:{dbfn}?mode=ro&vfs=zstd' -cmd .dump -cmd .exit",
        check=True,
        shell=True,
        cwd=tmpdir,
        stdout=subprocess.PIPE,
        text=True,
    )
    actual = [stmt.strip() for stmt in rslt.stdout.split(";\n")]
    assert actual == expected

    # test reopen through symlink
    linkpath = os.path.join(tmpdir, "symlink")
    os.symlink(dbfn, linkpath)
    assert os.path.realpath(linkpath) == os.path.realpath(dbfn)
    linkcon = sqlite3.connect(f"file:{linkpath}?vfs=zstd", uri=True)
    linkcon.execute("select 1")

    # verify application_id
    outer = sqlite3.connect(dbfn)
    assert next(outer.execute("PRAGMA application_id"))[0] == 0x7A737464


@pytest.mark.skipif(os.geteuid() != 0, reason="must run in docker")
def test_db_in_root():
    # regression test, create db with relative path to root directory
    # (this tends to happen in docker)
    con = sqlite3.connect(":memory:")
    con.enable_load_extension(True)
    con.load_extension(os.path.join(BUILD, "zstd_vfs"))
    os.chdir("/")
    con = sqlite3.connect("file:test_in_root.db?vfs=zstd", uri=True)
    con.executescript("CREATE TABLE hello(x INTEGER)")
    con.commit()


def test_vacuum(tmpdir, chinook_file):
    # open the zstd database
    con = sqlite3.connect(f"file:{chinook_file}?mode=ro", uri=True)
    con.enable_load_extension(True)
    con.load_extension(os.path.join(BUILD, "zstd_vfs"))
    dbfn = os.path.join(tmpdir, "chinook_zstd_vacuum.sqlite")
    con.execute(f"VACUUM INTO 'file:{dbfn}?vfs=zstd'")

    # verify application_id
    outer = sqlite3.connect(dbfn)
    assert next(outer.execute("PRAGMA application_id"))[0] == 0x7A737464

    # dump from the zstd version
    rslt = subprocess.run(
        f"sqlite3 :memory: -bail -cmd '.load {os.path.join(BUILD, 'zstd_vfs')}' -cmd '.open file:{dbfn}?mode=ro&vfs=zstd' -cmd .dump -cmd .exit",
        check=True,
        shell=True,
        cwd=tmpdir,
        stdout=subprocess.PIPE,
        text=True,
    )
    actual = [stmt.strip() for stmt in rslt.stdout.split(";\n")]

    # dump from the original database; the vacuumed copy's dump must equal it
    # (compare content, not a hardcoded statement count: the fixture is an
    # unpinned third-party download whose dump composition drifts)
    rslt = subprocess.run(
        f"sqlite3 :memory: -bail -cmd '.open file:{chinook_file}?mode=ro' -cmd .dump -cmd .exit",
        check=True,
        shell=True,
        cwd=tmpdir,
        stdout=subprocess.PIPE,
        text=True,
    )
    expected = [stmt.strip() for stmt in rslt.stdout.split(";\n")]
    assert actual == expected


def test_sam(tmpdir):
    (region, expected_posflag) = ("chr21:20000000-22500000", 12601293751857)
    # (region, expected_posflag) = ("chr21:20000000-25000000", 27074190881221)
    # (region, expected_posflag) = ("chr21:20000000-40000000", 148853599470365)
    page_size = 16384
    outer_page_size = 65536
    level = 9
    bam_path = os.path.join(os.path.dirname(__file__), "NA12878.chr21:20000000-22500000.bam")
    subprocess.run(
        f"samtools view {shlex.quote(bam_path)} | zstd -8 -T0 - -o {region}.sam.zst",
        check=True,
        shell=True,
        cwd=tmpdir,
    )
    with open(os.path.join(tmpdir, "sam2sql.awk"), "w") as outfile:
        outfile.write(
            """
            BEGIN {
                print "PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA locking_mode=EXCLUSIVE; PRAGMA auto_vacuum=FULL;";
                print "BEGIN;";
                print "CREATE TABLE sam(qname TEXT NOT NULL, flag INTEGER NOT NULL, rname TEXT, pos INTEGER, mapq INTEGER, cigar TEXT, rnext TEXT, pnext INTEGER, tlen INTEGER, seq TEXT NOT NULL, qual TEXT, tags TEXT, rg TEXT);"
            }
            {
                printf("INSERT INTO sam(qname,flag,rname,pos,mapq,cigar,rnext,pnext,tlen,seq,qual,tags,rg) VALUES('%s',%d,'%s',%d,%d,'%s','%s',%d,%d,'%s','%s','",$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                for (i=12; i<NF; i++) {
                    printf("%s\\t",$i);
                }
                printf("','%s');\\n",substr($NF,6));
            }
            END {
                #print "CREATE INDEX sam_pos on sam(rname,pos);"
                print "COMMIT;";
            }
        """
        )
    # NOTE: PRAGMA page_size is fed as the first line of the SQL stream rather than via
    # -cmd: sqlite3 shell >= 3.52 exits immediately after an SQL -cmd when -bail is set,
    # without ever reading stdin (upstream shell regression; dot-command -cmds unaffected).
    subprocess.run(
        f"""
        {{ echo 'PRAGMA page_size={page_size};'; zstd -dc {region}.sam.zst | sed s/\\'/\\'\\'/g | awk -f sam2sql.awk; }} \
            | sqlite3 :memory: -bail \
                -cmd '.load {os.path.join(BUILD, "zstd_vfs")}' \
                -cmd '.open file:{region}.zstd.sqlite?vfs=zstd&outer_unsafe=true&outer_page_size={outer_page_size}&level={level}'
        """,
        check=True,
        shell=True,
        cwd=tmpdir,
    )
    subprocess.run(
        f"""
        {{ echo 'PRAGMA page_size={page_size};'; zstd -dc {region}.sam.zst | sed s/\\'/\\'\\'/g | awk -f sam2sql.awk; }} \
            | sqlite3 :memory: -bail \
                -cmd '.load {os.path.join(BUILD, "zstd_vfs")}' \
                -cmd '.open file:{region}.zstd.nodict.sqlite?vfs=zstd&outer_unsafe=true&outer_page_size={outer_page_size}&level={level}&dict=false'
        """,
        check=True,
        shell=True,
        cwd=tmpdir,
    )
    subprocess.run(
        f"zstd -8 -T0 -c {region}.zstd.sqlite > {region}.zstd.sqlite.zst",
        check=True,
        shell=True,
        cwd=tmpdir,
    )
    subprocess.run(["ls", "-lh"], cwd=tmpdir, check=False)

    # verify db file size is in the ballpark of the sam.zst file size
    sam_zst = os.path.join(tmpdir, f"{region}.sam.zst")
    sam_zst_size = os.path.getsize(sam_zst)
    zstd_sqlite = os.path.join(tmpdir, f"{region}.zstd.sqlite")
    zstd_sqlite_size = os.path.getsize(zstd_sqlite)
    ratio = float(zstd_sqlite_size) / sam_zst_size
    assert ratio <= 1.6

    # verify outer page size and application_id
    con = sqlite3.connect(f"file:{zstd_sqlite}?mode=ro", uri=True)
    assert next(con.execute("PRAGMA page_size"))[0] == outer_page_size
    assert next(con.execute("PRAGMA application_id"))[0] == 0x7A737464
    btree_interior_pages_heuristic = set(
        con.execute(
            "select pageno from nested_vfs_zstd_pages indexed by nested_vfs_zstd_pages_btree_interior where btree_interior"
        )
    )
    assert 10 < len(btree_interior_pages_heuristic) < 100

    # verify inner page size
    con.enable_load_extension(True)
    con.load_extension(os.path.join(BUILD, "zstd_vfs"))
    con = sqlite3.connect(f"file:{zstd_sqlite}?mode=ro&vfs=zstd", uri=True)
    assert next(con.execute("PRAGMA page_size"))[0] == page_size

    # verify btree interior page index
    try:
        btree_interior_pages_actual = set(
            con.execute("select pageno from dbstat where pagetype='internal'")
        )
        # this won't necessarily hold for all databases, as the heuristic admits false positives;
        # but it does for this one:
        assert btree_interior_pages_heuristic == btree_interior_pages_actual
    except sqlite3.OperationalError as exn:
        assert "no such table: dbstat" in str(exn)  # tolerate absence from system build

    if expected_posflag:
        con.execute("PRAGMA threads=8")
        assert next(con.execute("select sum(pos+flag) from sam"))[0] == expected_posflag


def _connect_vfs(dbfn, vfs, **kwargs):
    con0 = sqlite3.connect(":memory:")
    con0.enable_load_extension(True)
    con0.load_extension(os.path.join(BUILD, vfs + "_vfs"))
    return sqlite3.connect(f"file:{dbfn}?vfs={vfs}", uri=True, isolation_level=None, **kwargs)


# Child process for the synchronous=OFF kill tests: performs the requested writes,
# prints READY, then idles so the parent can SIGKILL it with no chance to close.
_SYNC_KILL_CHILD = """
import os
import sqlite3
import sys
import time

build, dbfn, vfs, sync, shape = sys.argv[1:6]
con0 = sqlite3.connect(":memory:")
con0.enable_load_extension(True)
con0.load_extension(os.path.join(build, vfs + "_vfs"))
con = sqlite3.connect(f"file:{dbfn}?vfs={vfs}", uri=True, isolation_level=None)
if shape == "exclusive":
    con.execute("PRAGMA locking_mode=EXCLUSIVE")
con.execute(f"PRAGMA synchronous={sync}")
if shape in ("commits", "exclusive"):
    con.execute("CREATE TABLE t (x INTEGER)")
    for v in (1, 2, 3):
        con.execute("INSERT INTO t VALUES (?)", (v,))
elif shape == "midtxn":
    con.execute("CREATE TABLE t (x TEXT)")
    con.execute("INSERT INTO t VALUES ('committed')")
    con.execute("PRAGMA cache_size=10")
    con.execute("BEGIN")
    con.executemany("INSERT INTO t VALUES (?)", [("y" * 200,)] * 3000)
print("READY", flush=True)
while True:
    time.sleep(60)
"""


def _spawn_sync_child(tmpdir, vfs, sync, shape):
    dbfn = str(tmpdir.join(f"sync_{vfs}_{sync}_{shape}.db"))
    child_py = str(tmpdir.join("sync_kill_child.py"))
    with open(child_py, "w") as out:
        out.write(_SYNC_KILL_CHILD)
    errfn = str(tmpdir.join("sync_kill_child.err"))
    with open(errfn, "wb") as err:
        proc = subprocess.Popen(
            [sys.executable, child_py, BUILD, dbfn, vfs, sync, shape],
            stdout=subprocess.PIPE,
            stderr=err,
            text=True,
        )
    if proc.stdout.readline().strip() != "READY":
        with open(errfn) as err:
            raise AssertionError("sync kill-test child failed to start:\n" + err.read())
    return proc, dbfn


def _kill(proc):
    proc.kill()  # SIGKILL: no cleanup, no close
    proc.wait()


@pytest.mark.parametrize("vfs", ["nested", "zstd"])
def test_sync_off_commits_survive_process_kill(tmpdir, vfs):
    # under synchronous=OFF, a commit must reach the outer database when its write
    # transaction ends -- not only at close -- so abrupt process death loses nothing
    # that was acknowledged, including the schema itself
    proc, dbfn = _spawn_sync_child(tmpdir, vfs, "OFF", "commits")
    _kill(proc)
    con = _connect_vfs(dbfn, vfs)
    assert next(con.execute("PRAGMA integrity_check"))[0] == "ok"
    assert [r[0] for r in con.execute("SELECT x FROM t ORDER BY x")] == [1, 2, 3]
    con.close()


@pytest.mark.parametrize("vfs", ["nested", "zstd"])
def test_sync_normal_commits_survive_process_kill(tmpdir, vfs):
    # control: NORMAL commits were already durable per commit before this behavior
    # was extended to OFF
    proc, dbfn = _spawn_sync_child(tmpdir, vfs, "NORMAL", "commits")
    _kill(proc)
    con = _connect_vfs(dbfn, vfs)
    assert next(con.execute("PRAGMA integrity_check"))[0] == "ok"
    assert [r[0] for r in con.execute("SELECT x FROM t ORDER BY x")] == [1, 2, 3]
    con.close()


@pytest.mark.parametrize("vfs", ["nested", "zstd"])
def test_sync_off_kill_mid_transaction(tmpdir, vfs):
    # death inside a later, cache-spilling transaction must expose exactly the last
    # committed state: the earlier commit survives, the in-flight one leaves no trace
    proc, dbfn = _spawn_sync_child(tmpdir, vfs, "OFF", "midtxn")
    _kill(proc)
    con = _connect_vfs(dbfn, vfs)
    assert next(con.execute("PRAGMA integrity_check"))[0] == "ok"
    assert [r[0] for r in con.execute("SELECT x FROM t")] == ["committed"]
    con.close()


@pytest.mark.parametrize("sync", ["OFF", "NORMAL"])
def test_sync_commit_excludes_other_connections_until_close(tmpdir, sync):
    # once a writer commits, the documented EXCLUSIVE outer locking excludes
    # other connections for the writer's lifetime, in every synchronous mode;
    # after it closes, the committed data is there. OFF previously deviated:
    # its commits sat in the writer's still-open outer transaction, so a second
    # connection could open and silently read the pre-commit state instead.
    dbfn = str(tmpdir.join(f"sync_{sync.lower()}_exclusion.db"))
    base = _connect_vfs(dbfn, "nested")
    base.execute("CREATE TABLE t (x INTEGER)")
    base.execute("INSERT INTO t VALUES (1)")
    base.close()

    con1 = _connect_vfs(dbfn, "nested")
    con1.execute(f"PRAGMA synchronous={sync}")
    con1.execute("BEGIN")
    con1.execute("INSERT INTO t VALUES (2)")
    con1.execute("COMMIT")
    with pytest.raises(sqlite3.OperationalError, match="unable to open"):
        con2 = _connect_vfs(dbfn, "nested")
        con2.execute("SELECT sum(x) FROM t").fetchone()
    con1.close()

    con2 = _connect_vfs(dbfn, "nested")
    assert next(con2.execute("SELECT sum(x) FROM t"))[0] == 3
    con2.close()


def test_sync_off_exclusive_locking_takes_effect_at_close(tmpdir):
    # residue: a connection that itself sets locking_mode=EXCLUSIVE drops no lock
    # until close, so its OFF commits still take effect only at close -- but they
    # do take effect then
    dbfn = str(tmpdir.join("sync_off_exclusive.db"))
    con = _connect_vfs(dbfn, "nested")
    con.execute("PRAGMA locking_mode=EXCLUSIVE")
    con.execute("PRAGMA synchronous=OFF")
    con.execute("CREATE TABLE t (x INTEGER)")
    for v in (1, 2, 3):
        con.execute("INSERT INTO t VALUES (?)", (v,))
    con.close()
    con = _connect_vfs(dbfn, "nested")
    assert next(con.execute("PRAGMA integrity_check"))[0] == "ok"
    assert [r[0] for r in con.execute("SELECT x FROM t ORDER BY x")] == [1, 2, 3]
    con.close()


@contextlib.contextmanager
def report_elapsed(msg, file=sys.stderr):
    t0 = time.time()
    yield
    t = time.time() - t0
    print(f"{round(t, 3)}{msg}", file=file)


def test_tpch(tmpdir):
    results = subprocess.run(
        [
            "python3",
            os.path.join(HERE, "TPC-H.py"),
            "--cache-MiB",
            "100",
            "--inner-page-KiB",
            "64",
            "--outer-page-KiB",
            "2",
            "--level",
            "6",
            "--threads",
            str(max(multiprocessing.cpu_count(), 2)),
        ],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    print(results.stdout)
    results = json.loads(results.stdout)
    assert results["zstd_db_size"] * 2 < results["db_size"]
    assert results["Q1"] * 2 > results["zstd_Q1"]
    assert results["Q8"] * 4 > results["zstd_Q8"]


def test_web():
    # compressed db served from GitHub Releases. if we change the db schema then we'll need to
    # update accordingly (with a version generated locally by TPC-H.py)
    DB_URL = (
        "https://github.com/mlin/sqlite_zstd_vfs/releases/download/web-test-db-v3/TPC-H.zstd.db"
    )
    con = sqlite3.connect(":memory:")
    con.enable_load_extension(True)
    con.load_extension(os.path.join(BUILD, "zstd_vfs"))
    con = sqlite3.connect(
        f"file:/__web__?vfs=zstd&mode=ro&immutable=1&web_url={urllib.parse.quote(DB_URL)}", uri=True
    )
    con.executescript("PRAGMA cache_size=-262144")

    schema = list(con.execute("select type, name from sqlite_master"))
    print(schema)
    sys.stdout.flush()

    results = list(
        con.execute(
            """
            select
                l_returnflag,
                l_linestatus,
                sum(l_quantity) as sum_qty,
                sum(l_extendedprice) as sum_base_price,
                sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
                sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
                avg(l_quantity) as avg_qty,
                avg(l_extendedprice) as avg_price,
                avg(l_discount) as avg_disc, count(*) as count_order
            from lineitem
            where l_shipdate <= date('1998-12-01', '-90 day')
            group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;
            """
        )
    )
    print(results)
    sys.stdout.flush()

    # TODO: verify that btree interior index was used (currently, watch stderr for "interior: K")
