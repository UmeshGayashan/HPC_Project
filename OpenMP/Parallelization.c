#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include <omp.h>

#define DB_CONN "host=localhost dbname=hpc_search user=postgres password=583864"

void process_result_row(PGresult *res, int row_num, int thread_id) {
    printf("-----------------------------------------\n");
    printf("Thread %d: Result Row %d\n", thread_id, row_num);
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

void parallel_search(const char *userid, int thread_id) {
    PGconn *conn = PQconnectdb(DB_CONN);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Thread %d: Connection failed: %s\n", thread_id, PQerrorMessage(conn));
        PQfinish(conn);
        return;
    }

    char query[256];
    snprintf(query, sizeof(query), "SELECT * FROM reviews WHERE \"UserId\" = '%s'", userid);

    PGresult *result = PQexec(conn, query);
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Thread %d: Query failed for UserId %s: %s\n", thread_id, userid, PQerrorMessage(conn));
        PQclear(result);
        PQfinish(conn);
        return;
    }

    int total_matches = PQntuples(result);
    printf("\nThread %d: Found %d records for UserId: %s\n",thread_id, total_matches, userid);

    for (int i = 0; i < total_matches; i++) {
        process_result_row(result, i, thread_id);
    }

    PQclear(result);
    PQfinish(conn);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <search_userids>\n", argv[0]);
        return 1;
    }

    int userid_count = argc - 1;

    double start_time = omp_get_wtime();
    
    // Each thread processes one UserID
    #pragma omp parallel for num_threads(userid_count)
    for (int i = 0; i < userid_count; i++) {
        int thread_id = omp_get_thread_num();
        const char *userid = argv[i + 1];
        parallel_search(userid, thread_id);
    }

    double end_time = omp_get_wtime();
    printf("\nTotal execution time (Threads: %d): %.6f seconds\n", userid_count, end_time - start_time);


    return 0;
}
