#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include <omp.h>
#define DB_CONN "host=localhost dbname=hpc_search user=postgres password=583864"
#define THREAD_COUNT 1

void process_result_row(PGresult *res, int row_num, int thread_id) {
    printf("*****************************************\n");
    printf("Thread %d: Found match at row %d\n", thread_id, row_num);
    printf("ID: %s\n* ProductID: %s\n* UserID: %s\n* ProfileName: %s\n"
           "* HelpfulnessNumerator: %s\n* HelpfulnessDenominator: %s\n"
           "* Score: %s\n* Time: %s\n* Summary: %s\n* Text: %s\n",
           PQgetvalue(res, row_num, 0),
           PQgetvalue(res, row_num, 1),
           PQgetvalue(res, row_num, 2),
           PQgetvalue(res, row_num, 3),
           PQgetvalue(res, row_num, 4),
           PQgetvalue(res, row_num, 5),
           PQgetvalue(res, row_num, 6),
           PQgetvalue(res, row_num, 7),
           PQgetvalue(res, row_num, 8),
           PQgetvalue(res, row_num, 9));
}

void parallel_search(const char *search_userid) {
    PGconn *conn = PQconnectdb(DB_CONN);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return;
    }

    double start_time = omp_get_wtime();

    char query[256];
    snprintf(query, sizeof(query),
             "SELECT * FROM reviews WHERE \"UserId\" = '%s'", search_userid);

    PGresult *result = PQexec(conn, query);

    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Query failed: %s\n", PQerrorMessage(conn));
        PQclear(result);
        PQfinish(conn);
        return;
    }

    int total_matches = PQntuples(result);
    printf("Found %d matches for UserID: %s\n", total_matches, search_userid);

    omp_set_num_threads(THREAD_COUNT);

    #pragma omp parallel for
    for (int i = 0; i < total_matches; i++) {
        int thread_id = omp_get_thread_num();
        process_result_row(result, i, thread_id);
    }

    double end_time = omp_get_wtime();
    printf("\nQuery execution time (Number of Threads: %d): %.6f seconds\n", THREAD_COUNT, end_time - start_time);

    PQclear(result);
    PQfinish(conn);
}

int main(int argc, char **argv) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <search_userid>\n", argv[0]);
        return 1;
    }

    const char *search_userid = argv[1];

    parallel_search(search_userid);

    return 0;
}
