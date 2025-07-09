#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <time.h> 

// Basic database search function
PGresult* search_by_id(PGconn *conn, const char *search_id) {
    char query[256];
    snprintf(query, sizeof(query), "SELECT * FROM reviews WHERE \"UserId\" = '%s'", search_id);

    PGresult *result = PQexec(conn, query);
    
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Query failed: %s: %s", search_id, PQerrorMessage(conn));
        PQclear(result);
        return NULL;
    }
    
    return result;
}

int main(int argc, char **argv) {
    // Check if the search UserId is provided as an argument
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <search_UserIds>\n", argv[0]);
        return 1;
    }

    PGconn *conn = PQconnectdb("host=localhost dbname=hpc_search user=postgres password=583864");
    
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }

    // Start measuring time
    clock_t start_time = clock();

    for (int i = 1; i < argc; i++) {
        const char *search_id = argv[i];
        printf("\nSearching for UserId: %s\n", search_id);

        PGresult *result = search_by_id(conn, search_id);

        if (result) {
            int rows = PQntuples(result);
            printf("Found %d records for UserId %s:\n", rows, search_id);

            for (int j = 0; j < rows; j++) {
                printf("ID: %s\n* ProductID: %s\n* UserID: %s\n* ProfileName: %s\n* HelpfulnessNumerator: %s\n* HelpfulnessDenominator: %s\n* Score: %s\n* Time: %s\n* Summary: %s\n* Text: %s\n\n",
                       PQgetvalue(result, j, 0),
                       PQgetvalue(result, j, 1),
                       PQgetvalue(result, j, 2),
                       PQgetvalue(result, j, 3),
                       PQgetvalue(result, j, 4),
                       PQgetvalue(result, j, 5),
                       PQgetvalue(result, j, 6),
                       PQgetvalue(result, j, 7),
                       PQgetvalue(result, j, 8),
                       PQgetvalue(result, j, 9));
            }

            PQclear(result);
        }
    }

    // End measuring time
    clock_t end_time = clock();
    double time_taken = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;

    printf("Query execution time: %.6f seconds\n", time_taken);
    
    
    PQfinish(conn);
    return 0;
}