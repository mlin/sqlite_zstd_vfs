#!/usr/bin/env python3
import sys
import os
import subprocess
import contextlib
import time
import sqlite3
import argparse
import json

HERE = os.path.dirname(__file__)
BUILD = os.path.abspath(os.path.join(HERE, "..", "build"))

DB_URL = "https://github.com/lovasoa/TPCH-sqlite/releases/download/v1.0/TPC-H.db"
queries = {}
queries[
    "Q1"
] = """
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
queries[
    "Q8"
] = """
    select
        o_year,
        sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share
    from
        (select
            strftime("%Y", o_orderdate) as o_year,
            l_extendedprice * (1-l_discount) as volume,
            n2.n_name as nation
        from part, supplier, lineitem, orders, customer, nation n1, nation n2, region
        where
            p_partkey = l_partkey
            and s_suppkey = l_suppkey
            and l_orderkey = o_orderkey
            and o_custkey = c_custkey
            and c_nationkey = n1.n_nationkey
            and n1.n_regionkey = r_regionkey
            and r_name = 'AMERICA'
            and s_nationkey = n2.n_nationkey
            and o_orderdate between date('1995-01-01') and date('1996-12-31')
            and p_type = 'ECONOMY ANODIZED STEEL')
        as all_nations
    group by o_year order by o_year;
    """


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache-MiB", type=int, help="SQLite page cache size (default ~2)")
    parser.add_argument("--level", type=int, help="zstd compression level (default 3)", default=3)
    parser.add_argument(
        "--inner-page-KiB",
        type=int,
        help="inner db page size (before compression; default 4)",
        default=4,
    )
    parser.add_argument(
        "--outer-page-KiB", type=int, help="outer db page size (default 4)", default=4
    )
    args = parser.parse_args(sys.argv[1:])

    ans = run(args.cache_MiB, args.level, args.inner_page_KiB, args.outer_page_KiB)
    print(json.dumps(ans, indent=2))


def run(cache_MiB, level, inner_page_KiB, outer_page_KiB):
    # download db to /tmp/TPC-H.db; --continue=true avoids re-download
    subprocess.run(
        f"aria2c -x 10 -j 10 -s 10 --file-allocation=none --continue=true {DB_URL} >&2",
        check=True,
        shell=True,
        cwd="/tmp",
    )

    # VACUUM INTO a fresh (uncompressed) copy
    timings = {}
    try:
        os.unlink("/tmp/TPC-H.vacuum.db")
    except FileNotFoundError:
        pass
    subprocess.run("cat /tmp/TPC-H.db > /dev/null", check=True, shell=True)
    con = sqlite3.connect(f"file:/tmp/TPC-H.db?mode=ro", uri=True)
    with timer(timings, "load"):
        con.execute(f"PRAGMA page_size={1024*inner_page_KiB}")
        con.execute("VACUUM INTO '/tmp/TPC-H.vacuum.db'")
        con.close()

    # run each query twice, with a fresh db connection, and measure the second run; so it should
    # have a hot filesystem cache and cold db page cache.
    expected_results = {}
    for query_name, query_sql in queries.items():
        con = sqlite3.connect(f"file:/tmp/TPC-H.vacuum.db?mode=ro", uri=True)
        expected_results[query_name] = list(con.execute(query_sql))
        con.close()

        con = sqlite3.connect(f"file:/tmp/TPC-H.vacuum.db?mode=ro", uri=True)
        if cache_MiB:
            con.execute(f"PRAGMA cache_size={-1024*cache_MiB}")
        with timer(timings, query_name):
            results = list(con.execute(query_sql))
        con.close()
        assert results == expected_results[query_name]

    # create zstd-compressed db using VACUUM INTO
    try:
        os.unlink("/tmp/TPC-H.zstd.db")
    except FileNotFoundError:
        pass
    con = sqlite3.connect(f"file:/tmp/TPC-H.vacuum.db?mode=ro", uri=True)
    con.enable_load_extension(True)
    con.load_extension(os.path.join(BUILD, "zstd_vfs"))
    with timer(timings, "load_zstd"):
        con.execute(f"PRAGMA page_size={1024*inner_page_KiB}")
        con.execute(
            f"VACUUM INTO 'file:/tmp/TPC-H.zstd.db?vfs=zstd&outer_unsafe=true&outer_page_size={1024*outer_page_KiB}&level={level}'"
        )
        con.close()
    timings["db_size"] = os.path.getsize("/tmp/TPC-H.vacuum.db")
    timings["zstd_db_size"] = os.path.getsize("/tmp/TPC-H.zstd.db")

    # repeat queries on compressed db
    for query_name, query_sql in queries.items():
        con = connect_zstd("/tmp/TPC-H.zstd.db", cache_MiB=cache_MiB)
        results = list(con.execute(query_sql))
        con.close()
        assert results == expected_results[query_name]

        con = connect_zstd("/tmp/TPC-H.zstd.db", cache_MiB=cache_MiB)
        with timer(timings, "zstd_" + query_name):
            results = list(con.execute(query_sql))
        con.close()
        assert results == expected_results[query_name]

    return timings


def connect_zstd(dbfn, mode="ro", cache_MiB=None):
    con = sqlite3.connect(f":memory:")
    con.enable_load_extension(True)
    con.load_extension(os.path.join(BUILD, "zstd_vfs"))
    con = sqlite3.connect(f"file:{dbfn}?vfs=zstd&mode={mode}", uri=True)
    if cache_MiB:
        con.execute(f"PRAGMA cache_size={-1024*cache_MiB}")
    return con


@contextlib.contextmanager
def timer(timings_dict, name):
    t0 = time.time()
    yield
    timings_dict[name] = round(time.time() - t0, 3)


if __name__ == "__main__":
    main()
