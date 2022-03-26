import sys
import os
import subprocess
import multiprocessing
import tempfile
import json
import contextlib
import time
import sqlite3
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
        universal_newlines=True,
    )
    expected = [stmt.strip() for stmt in rslt.stdout.split(";\n")]

    # open the nested database
    con = sqlite3.connect(f":memory:")
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
        f"sqlite3 :memory: -bail -cmd '.load {os.path.join(BUILD,'nested_vfs')}' -cmd '.open file:{dbfn}?mode=ro&vfs=nested' -cmd .dump -cmd .exit",
        check=True,
        shell=True,
        cwd=tmpdir,
        stdout=subprocess.PIPE,
        universal_newlines=True,
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
        universal_newlines=True,
    )
    expected = [stmt.strip() for stmt in rslt.stdout.split(";\n")]
    assert len(expected) == 15632

    # open the nested database
    con = sqlite3.connect(f":memory:")
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
        f"sqlite3 :memory: -bail -cmd '.load {os.path.join(BUILD,'zstd_vfs')}' -cmd '.open file:{dbfn}?mode=ro&vfs=zstd' -cmd .dump -cmd .exit",
        check=True,
        shell=True,
        cwd=tmpdir,
        stdout=subprocess.PIPE,
        universal_newlines=True,
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
    con = sqlite3.connect(f":memory:")
    con.enable_load_extension(True)
    con.load_extension(os.path.join(BUILD, "zstd_vfs"))
    os.chdir("/")
    con = sqlite3.connect(f"file:test_in_root.db?vfs=zstd", uri=True)
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
        f"sqlite3 :memory: -bail -cmd '.load {os.path.join(BUILD,'zstd_vfs')}' -cmd '.open file:{dbfn}?mode=ro&vfs=zstd' -cmd .dump -cmd .exit",
        check=True,
        shell=True,
        cwd=tmpdir,
        stdout=subprocess.PIPE,
        universal_newlines=True,
    )
    actual = [stmt.strip() for stmt in rslt.stdout.split(";\n")]
    assert len(actual) == 15632


def test_sam(tmpdir):
    (region, expected_posflag) = ("chr21:20000000-25000000", 27074190881221)
    # (region, expected_posflag) = ("chr21:20000000-40000000", 148853599470365)
    page_size = 16384
    outer_page_size = 65536
    level = 9
    subprocess.run(
        f"samtools view -O BAM -@ 4 -o {region}.bam https://s3.amazonaws.com/1000genomes/1000G_2504_high_coverage/data/ERR3239334/NA12878.final.cram {region}",
        check=True,
        shell=True,
        cwd=tmpdir,
    )
    subprocess.run(
        f"samtools view {region}.bam | zstd -8 -T0 - -o {region}.sam.zst",
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
    subprocess.run(
        f"""
        zstd -dc {region}.sam.zst | sed s/\\'/\\'\\'/g | awk -f sam2sql.awk \
            | sqlite3 :memory: -bail \
                -cmd '.load {os.path.join(BUILD,'zstd_vfs')}' \
                -cmd '.open file:{region}.zstd.sqlite?vfs=zstd&outer_unsafe=true&outer_page_size={outer_page_size}&level={level}' \
                -cmd 'PRAGMA page_size={page_size}'
        """,
        check=True,
        shell=True,
        cwd=tmpdir,
    )
    subprocess.run(
        f"""
        zstd -dc {region}.sam.zst | sed s/\\'/\\'\\'/g | awk -f sam2sql.awk \
            | sqlite3 :memory: -bail \
                -cmd '.load {os.path.join(BUILD,'zstd_vfs')}' \
                -cmd '.open file:{region}.zstd.nodict.sqlite?vfs=zstd&outer_unsafe=true&outer_page_size={outer_page_size}&level={level}&dict=false' \
                -cmd 'PRAGMA page_size={page_size}'
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
    subprocess.run(["ls", "-lh"], cwd=tmpdir)

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


@contextlib.contextmanager
def report_elapsed(msg, file=sys.stderr):
    t0 = time.time()
    yield
    t = time.time() - t0
    print(f"{round(t,3)}{msg}", file=file)


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
        universal_newlines=True,
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
    con = sqlite3.connect(f":memory:")
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
