#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <libpq-fe.h>

#define DB_CONN "host=localhost dbname=hpc_search user=postgres password=583864"

void process_result_row(PGresult *res, int row_num, int rank) {
    printf("-----------------------------------------\n");
    printf("Process %d: Result Row %d\n", rank, row_num);
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

void mpi_search_userid(const char *userid, int rank) {
    PGconn *conn = PQconnectdb(DB_CONN);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Process %d: Connection failed: %s\n", rank, PQerrorMessage(conn));
        PQfinish(conn);
        return;
    }

    char query[256];
    snprintf(query, sizeof(query), "SELECT * FROM reviews WHERE \"UserId\" = '%s'", userid);

    PGresult *res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Process %d: Query failed for UserId %s: %s\n", rank, userid, PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return;
    }

    int rows = PQntuples(res);
    printf("\nProcess %d: Found %d records for UserId: %s\n", rank, rows, userid);

    for (int i = 0; i < rows; i++) {
        process_result_row(res, i, rank);
    }

    PQclear(res);
    PQfinish(conn);
}

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int userid_count = argc - 1;

    if (rank == 0 && userid_count < 1) {
        fprintf(stderr, "Usage: mpirun -np <N> %s <UserIds>\n", argv[0]);
        MPI_Finalize();
        return 1;
    }

    double start_time = MPI_Wtime();

    if (rank < userid_count) {
        const char *userid = argv[rank + 1];
        mpi_search_userid(userid, rank);
    } else {
        printf("Process %d: No UserID assigned. Exiting.\n", rank);
    }

    double end_time = MPI_Wtime();

    // Compute total searching time across all processes
    double global_start, global_end;
    MPI_Reduce(&start_time, &global_start, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&end_time, &global_end, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    // all processes and delivers the result only to the root process
    if (rank == 0) {
        printf("\nTotal searching time across all processes (using %d processes): %.6f seconds\n", size, global_end - global_start);
    }
    MPI_Finalize();
    return 0;
}
