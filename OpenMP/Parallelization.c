#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include <omp.h>


#define DB_CONN "host=localhost dbname=hpc_search user=postgres password=583864"
#define THREAD_COUNT 6

void search_value(int thread_id, const char *search_userid, int offset, int limit) {
    PGconn *conn = PQconnectdb(DB_CONN);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Thread %d: Connection failed: %s\n", thread_id, PQerrorMessage(conn));
        PQfinish(conn);
        return;
    }

    char query[512];
    snprintf(query, sizeof(query),
             "SELECT * FROM reviews ORDER BY \"UserId\" OFFSET %d LIMIT %d", offset, limit);

    PGresult *res = PQexec(conn, query);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Thread %d: Query failed: %s\n", thread_id, PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return;
    }

    int rows = PQntuples(res);
    for (int i = 0; i < rows; i++) {
        const char *user_id = PQgetvalue(res, i, 2);
        if (strcmp(user_id, search_userid) == 0){
            printf("*****************************************\n");
            printf("Thread %d: Found User ID %s in offset %d\n", thread_id, search_userid, offset);
            printf("ID: %s\n* ProductID: %s\n* UserID: %s\n* ProfileName: %s\n* HelpfulnessNumerator: %s\n* HelpfulnessDenominator: %s\n* Score: %s\n* Time: %s\n* Summary: %s\n* Text: %s\n",
                   PQgetvalue(res, i, 0),
                   PQgetvalue(res, i, 1),
                   PQgetvalue(res, i, 2),
                   PQgetvalue(res, i, 3),
                   PQgetvalue(res, i, 4),
                   PQgetvalue(res, i, 5),
                   PQgetvalue(res, i, 6),
                   PQgetvalue(res, i, 7),
                   PQgetvalue(res, i, 8),
                   PQgetvalue(res, i, 9));
        }
    }

    PQclear(res);
    PQfinish(conn);
}

int get_total_rows() {
    PGconn *conn = PQconnectdb(DB_CONN);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection failed to count rows: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return 0;
    }

    PGresult *res = PQexec(conn, "SELECT COUNT(*) FROM reviews");

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Count query failed: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return 0;
    }

    int total = atoi(PQgetvalue(res, 0, 0));
    PQclear(res);
    PQfinish(conn);
    return total;
}

int main(int argc, char **argv) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <search_id>\n", argv[0]);
        return 1;
    }

    char *search_userid = argv[1];
    int total_rows = get_total_rows();

    if (total_rows == 0) {
        fprintf(stderr, "Could not fetch row count or table is empty.\n");
        return 1;
    }

    printf("Total rows: %d, Searching for User ID: %s using %d threads\n", total_rows, search_userid, THREAD_COUNT);
    
    // (a / b)	Rounds down
    // (a + b - 1) / b	Rounds up (ensures all data is used)
    int rows_per_thread = (total_rows + THREAD_COUNT - 1) / THREAD_COUNT;

    double start_time = omp_get_wtime();

    omp_set_num_threads(THREAD_COUNT);

    #pragma omp parallel
    {
        int thread_id = omp_get_thread_num();
        int offset = thread_id * rows_per_thread;
        int limit = rows_per_thread;

        // last thread does not exceed final row
        if (offset + limit > total_rows) {
            limit = total_rows - offset;
        }

        search_value(thread_id, search_userid, offset, limit);
    }

    double end_time = omp_get_wtime();
    printf("Total search time: %.6f seconds\n", (end_time - start_time));

    return 0;
}
