#include "duckdb.hpp"
#include <unistd.h>
#include <ctime>
#include <chrono>
#include <fstream>
#include <streambuf>
#include <regex>
#include <iostream>

using namespace duckdb;

void setupS3(Connection &con, string &s3_access_key, string &s3_secret_key, bool load_tpch = false) {
    // Load extension httpfs
    con.Query("INSTALL httpfs");
    con.Query("LOAD httpfs");

    con.Query("INSTALL parquet");
    con.Query("LOAD parquet");

    // Set S3 and create view
    con.Query("SET s3_region='us-west-2'");
    con.Query("SET s3_access_key_id='" + s3_access_key + "'");
    con.Query("SET s3_secret_access_key='" + s3_secret_key + "'");

    if (load_tpch) {
        con.Query("create or replace view s3_lineitem as SELECT * FROM read_parquet('s3://connector-x/redshift-unload/tpchsf10/parquet/lineitem/*')");
        con.Query("create or replace view s3_orders as SELECT * FROM read_parquet('s3://connector-x/redshift-unload/tpchsf10/parquet/orders/*')");
        con.Query("create or replace view s3_customer as SELECT * FROM read_parquet('s3://connector-x/redshift-unload/tpchsf10/parquet/customer/*')");
        con.Query("create or replace view s3_part as SELECT * FROM read_parquet('s3://connector-x/redshift-unload/tpchsf10/parquet/part/*')");
        con.Query("create or replace view s3_supplier as SELECT * FROM read_parquet('s3://connector-x/redshift-unload/tpchsf10/parquet/supplier/*')");
        con.Query("create or replace view s3_partsupp as SELECT * FROM read_parquet('s3://connector-x/redshift-unload/tpchsf10/parquet/partsupp/*')");
        con.Query("create or replace view s3_region as SELECT * FROM read_parquet('s3://connector-x/redshift-unload/tpchsf10/parquet/region/*')");
        con.Query("create or replace view s3_nation as SELECT * FROM read_parquet('s3://connector-x/redshift-unload/tpchsf10/parquet/nation/*')");
    }
}

void setupPGScan(Connection &con, string &pg_scan_ext, string &pg_conn, bool load_tpch = false) {
    // Load extension connectorx
    con.Query("LOAD '" + pg_scan_ext + "'");

    if (load_tpch) {
        con.Query("create or replace view pg_lineitem as SELECT * FROM POSTGRES_SCAN('" + pg_conn + "', 'public', 'lineitem')");
        con.Query("create or replace view pg_customer as SELECT * FROM POSTGRES_SCAN('" + pg_conn + "', 'public', 'customer')");
        con.Query("create or replace view pg_orders as SELECT * FROM POSTGRES_SCAN('" + pg_conn + "', 'public', 'orders')");
        con.Query("create or replace view pg_part as SELECT * FROM POSTGRES_SCAN('" + pg_conn + "', 'public', 'part')");
        con.Query("create or replace view pg_supplier as SELECT * FROM POSTGRES_SCAN('" + pg_conn + "', 'public', 'supplier')");
        con.Query("create or replace view pg_partsupp as SELECT * FROM POSTGRES_SCAN('" + pg_conn + "', 'public', 'partsupp')");
        con.Query("create or replace view pg_nation as SELECT * FROM POSTGRES_SCAN('" + pg_conn + "', 'public', 'nation')");
        con.Query("create or replace view pg_region as SELECT * FROM POSTGRES_SCAN('" + pg_conn + "', 'public', 'region')");
    }
}

void setupPGScanPushdown(Connection &con, string &pg_scan_ext, string &pg_conn, bool load_tpch = false) {
    // Load extension connectorx
    con.Query("LOAD '" + pg_scan_ext + "'");

    if (load_tpch) {
        con.Query("create or replace view pg_lineitem as SELECT * FROM POSTGRES_SCAN_PUSHDOWN('" + pg_conn + "', 'public', 'lineitem')");
        con.Query("create or replace view pg_customer as SELECT * FROM POSTGRES_SCAN_PUSHDOWN('" + pg_conn + "', 'public', 'customer')");
        con.Query("create or replace view pg_orders as SELECT * FROM POSTGRES_SCAN_PUSHDOWN('" + pg_conn + "', 'public', 'orders')");
        con.Query("create or replace view pg_part as SELECT * FROM POSTGRES_SCAN_PUSHDOWN('" + pg_conn + "', 'public', 'part')");
        con.Query("create or replace view pg_supplier as SELECT * FROM POSTGRES_SCAN_PUSHDOWN('" + pg_conn + "', 'public', 'supplier')");
        con.Query("create or replace view pg_partsupp as SELECT * FROM POSTGRES_SCAN_PUSHDOWN('" + pg_conn + "', 'public', 'partsupp')");
        con.Query("create or replace view pg_nation as SELECT * FROM POSTGRES_SCAN_PUSHDOWN('" + pg_conn + "', 'public', 'nation')");
        con.Query("create or replace view pg_region as SELECT * FROM POSTGRES_SCAN_PUSHDOWN('" + pg_conn + "', 'public', 'region')");
    }
}

void setupLocalParquet(Connection &con, string &local_path) {
    con.Query("INSTALL parquet");
    con.Query("LOAD parquet");

    con.Query("create or replace view lc_lineitem as SELECT * FROM read_parquet('" + local_path + "/lineitem/*')");
    con.Query("create or replace view lc_orders as SELECT * FROM read_parquet('" + local_path + "/orders/*')");
    con.Query("create or replace view lc_customer as SELECT * FROM read_parquet('" + local_path + "/customer/*')");
    con.Query("create or replace view lc_part as SELECT * FROM read_parquet('" + local_path + "/part/*')");
    con.Query("create or replace view lc_supplier as SELECT * FROM read_parquet('" + local_path + "/supplier/*')");
    con.Query("create or replace view lc_partsupp as SELECT * FROM read_parquet('" + local_path + "/partsupp/*')");
    con.Query("create or replace view lc_region as SELECT * FROM read_parquet('" + local_path + "/region/*')");
    con.Query("create or replace view lc_nation as SELECT * FROM read_parquet('" + local_path + "/nation/*')"); 
}

void run(Connection &con, string &query) {
    auto start = std::chrono::system_clock::now();
    auto res = con.Query(query);
    // con.Query("explain " + query)->Print();
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> duration = end - start;
    std::time_t start_time = std::chrono::system_clock::to_time_t(start);
    std::time_t end_time = std::chrono::system_clock::to_time_t(end);

    std::cout << "start: " << std::ctime(&start_time) << "\nend: " << std::ctime(&end_time) << "\nduration: " << duration.count() << "s" << std::endl;
    std::cout << "query result get " << res->RowCount() << " rows and " << res->ColumnCount() << " columns" << std::endl;
    // res->Print();
}

/**
* Parameters:
*   argv[1]: db_path
*   argv[2]: s3_access_key
*   argv[3]: s3_secret_key
*   argv[4]: pg_conn 
*   argv[5]: pg_scan_ext
*   argv[6]: query_path
*   argv[7]: local_path
**/
int main(int argc, char *argv[]) {
    DBConfig config;
    config.options.allow_unsigned_extensions = true;
    char *db_path = nullptr;
    assert(argc >= 8);

    if (argv[1][0] != '\0') {
	   db_path = argv[1];
    }
    string s3_access_key = string(argv[2]);
    string s3_secret_key = string(argv[3]);
    string pg_conn = string(argv[4]);
    string pg_scan_ext = string(argv[5]);
    string query_path = string(argv[6]);
    string local_path = string(argv[7]);

    std::ifstream tmp_stream(query_path);
    std::string query((std::istreambuf_iterator<char>(tmp_stream)),
                 std::istreambuf_iterator<char>());
    fprintf(stderr, "query: %s\n", query.c_str());

    DuckDB db(db_path, &config);
	Connection con(db);

    // Prepare S3
    // setupS3(con, s3_access_key, s3_secret_key, true);

    // Prepare local parquets
    // setupLocalParquet(con, local_path);

    // Prepare connectorx extension
    // setupPGScan(con, pg_scan_ext, pg_conn, true);
    setupPGScanPushdown(con, pg_scan_ext, pg_conn, true);

    // Run query
    run(con, query);

    return 0;
}
