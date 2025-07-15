#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include <mpi.h>
#include <omp.h>

#define DB_CONN "host=localhost dbname=hpc_search user=postgres password=583864"

void process_result_row(PGresult *res, int row_num, int rank, int thread_id) {
    printf("-----------------------------------------\n");
    printf("Process %d - Thread %d: Result Row %d\n", rank, thread_id, row_num);
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

void parallel_search(const char *userid, int rank, int thread_id) {
    PGconn *conn = PQconnectdb(DB_CONN);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Process %d - Thread %d: Connection failed: %s\n", rank, thread_id, PQerrorMessage(conn));
        PQfinish(conn);
        return;
    }

    char query[256];
    snprintf(query, sizeof(query), "SELECT * FROM reviews WHERE \"UserId\" = '%s'", userid);

    PGresult *res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Process %d - Thread %d: Query failed for UserId %s: %s\n", rank, thread_id, userid, PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return;
    }

    int rows = PQntuples(res);
    printf("\nProcess %d - Thread %d: Found %d records for UserId: %s\n", rank, thread_id, rows, userid);

    for (int i = 0; i < rows; i++) {
        process_result_row(res, i, rank, thread_id);
    }

    PQclear(res);
    PQfinish(conn);
}

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int total_userids = argc - 1;
    if (total_userids < size) {
        if (rank == 0) {
            fprintf(stderr, "Error: Number of UserIDs (%d) must be >= number of MPI processes (%d)\n", total_userids, size);
        }
        MPI_Finalize();
        return 1;
    }

    // Divide UserIDs across processes
    int chunk_size = (total_userids + size - 1) / size;
    int start_index = rank * chunk_size;
    int end_index = (start_index + chunk_size > total_userids) ? total_userids : start_index + chunk_size;
    int local_count = end_index - start_index;

    if (local_count <= 0) {
        printf("Process %d: No UserIDs to process.\n", rank);
        MPI_Finalize();
        return 0;
    }

    double local_start = MPI_Wtime();

    // Use OpenMP to parallelize within each process
    #pragma omp parallel for num_threads(local_count)
    for (int i = 0; i < local_count; i++) {
        int thread_id = omp_get_thread_num();
        const char *userid = argv[start_index + i + 1];
        parallel_search(userid, rank, thread_id);
    }

    double local_end = MPI_Wtime();

    double global_start, global_end;
    MPI_Reduce(&local_start, &global_start, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&local_end, &global_end, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        printf("\nHybrid MPI+OpenMP Execution Time (Processes: %d): %.6f seconds\n", size, global_end - global_start);
    }

    MPI_Finalize();
    return 0;
}
