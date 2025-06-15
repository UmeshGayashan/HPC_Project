#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <time.h>          // For timing

// Basic database search function
PGresult* search_by_id(PGconn *conn, const char *search_id) {
    char query[256];
    snprintf(query, sizeof(query), "SELECT * FROM reviews WHERE \"UserId\" = '%s'", search_id);

    PGresult *result = PQexec(conn, query);
    
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Query failed: %s", PQerrorMessage(conn));
        PQclear(result);
        return NULL;
    }
    
    return result;
}

int main(int argc, char **argv) {
    // Check if the search UserId is provided as an argument
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <search_UserId>\n", argv[0]);
        return 1;
    }

    char *search_id = argv[1];

    PGconn *conn = PQconnectdb("host=localhost dbname=hpc_search user=postgres password=583864");
    
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }

    // Start measuring time
    clock_t start_time = clock();

    PGresult *result = search_by_id(conn, search_id);

    // End measuring time
    clock_t end_time = clock();
    double time_taken = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;

    printf("Query execution time: %.6f seconds\n", time_taken);
    
    if (result) {
        int rows = PQntuples(result);
        printf("Found %d records:\n", rows);
        
        for(int i=0; i<rows; i++) {
            printf("ID: %s\n* ProductID: %s\n* UserID: %s\n* ProfileName: %s\n* HelpfulnessNumerator: %s\n* HelpfulnessDenominator: %s\n* Score: %s\n* Time: %s\n* Summary: %s\n* Text: %s\n",
                   PQgetvalue(result, i, 0),
                   PQgetvalue(result, i, 1),
                   PQgetvalue(result, i, 2),
                   PQgetvalue(result, i, 3),
                   PQgetvalue(result, i, 4),
                   PQgetvalue(result, i, 5),
                   PQgetvalue(result, i, 6),
                   PQgetvalue(result, i, 7),
                   PQgetvalue(result, i, 8),
                   PQgetvalue(result, i, 9));
        }
        PQclear(result);
    }
    
    PQfinish(conn);
    return 0;
}