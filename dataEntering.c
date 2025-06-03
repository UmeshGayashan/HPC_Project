#include <stdio.h>
#include <stdlib.h> // For memory allocation and conversion functions (e.g., strtol)
#include <string.h> // For string handling (e.g., strtok, strdup)
#include <libpq-fe.h> // PostgreSQL database connection functions
#include <ctype.h> // For character checks (e.g., isalnum)

#define MAX_COLS 50 // Maximum number of columns in CSV
#define MAX_LINE 2048 

// Replace spaces and non-alphanumeric characters in column names with underscores
void sanitize_column(char *col) {
    for (int i = 0; col[i]; i++) {
        if (col[i] == ' ') col[i] = '_';
        else if (!isalnum(col[i])) col[i] = '_';
    }
}

char* infer_type(char **samples, int count) {
    // Assume all values are integers and floats
    // If any sample fails to match integer or float, those flags are turned off (= 0).
    int int_flag = 1, float_flag = 1;
    
    for (int i = 0; i < count; i++) {
        // If this sample is NULL or empty, skip it
        if (!samples[i] || strlen(samples[i]) == 0) continue;
        
        char *end;

        // Check if the sample can be parsed as an integer or float
        // Converting to an integer (base 10)
        strtol(samples[i], &end, 10);
        if (*end != '\0') int_flag = 0;
        
        strtod(samples[i], &end);
        if (*end != '\0') float_flag = 0;
    }
    
    return int_flag ? "INTEGER" : float_flag ? "REAL" : "TEXT";
}

int main(int argc, char **argv) {
    // argument count check (./csv2pg Reviews.csv)
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <csv_file>\n", argv[0]);
        return 1;
    }

    // Open the CSV file --> argv[1]
    FILE *fp = fopen(argv[1], "r");
    if (!fp) {
        perror("File open error");
        return 1;
    }

    // Process header
    char header[MAX_LINE];
    fgets(header, MAX_LINE, fp);
    header[strcspn(header, "\r\n")] = 0;

    char *cols[MAX_COLS];
    int col_count = 0;
    char *tok = strtok(header, ",");
    while (tok && col_count < MAX_COLS) {
        cols[col_count] = strdup(tok);
        sanitize_column(cols[col_count]);
        col_count++;
        tok = strtok(NULL, ",");
    }

    // Sample data for type inference
    char *samples[MAX_COLS][10] = {0};
    int sample_rows = 0;
    char line[MAX_LINE];
    
    while (sample_rows < 10 && fgets(line, MAX_LINE, fp)) {
        line[strcspn(line, "\r\n")] = 0;
        char *vals[MAX_COLS] = {0};
        int idx = 0;
        char *v = strtok(line, ",");
        
        while (v && idx < col_count) {
            vals[idx] = v;
            idx++;
            v = strtok(NULL, ",");
        }
        
        for (int i = 0; i < col_count; i++) {
            samples[i][sample_rows] = vals[i] ? strdup(vals[i]) : NULL;
        }
        sample_rows++;
    }
    rewind(fp);

    // Extract table name from CSV filename
    char *filename = strrchr(argv[1], '/');  // Get last part of the path
    filename = filename ? filename + 1 : argv[1];  // Remove path if any

    char table_name[256];
    strncpy(table_name, filename, sizeof(table_name) - 1);
    table_name[sizeof(table_name) - 1] = '\0';

    // Remove file extension (e.g., ".csv")
    char *dot = strrchr(table_name, '.');
    if (dot) *dot = '\0';

    // Convert to lowercase
    for (int i = 0; table_name[i]; i++) {
        table_name[i] = tolower(table_name[i]);
    }

    // Generate CREATE TABLE
    char create_sql[4096];
    snprintf(create_sql, sizeof(create_sql), "CREATE TABLE IF NOT EXISTS \"%s\" (", table_name);
    for (int i = 0; i < col_count; i++) {
        char *type = infer_type(samples[i], sample_rows);
        sprintf(create_sql + strlen(create_sql), "\"%s\" %s%s", 
                cols[i], type, i < col_count-1 ? ", " : "");
    }
    strcat(create_sql, ");");

    // PostgreSQL connection
    PGconn *conn = PQconnectdb("host=localhost dbname=hpc_search user=postgres password=583864");
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection error: %s\n", PQerrorMessage(conn));
        return 1;
    }

    // Execute CREATE TABLE
    PGresult *res = PQexec(conn, create_sql);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Table creation failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }
    PQclear(res);

    // Prepare COPY command
    res = PQexec(conn, "COPY reviews FROM STDIN WITH (FORMAT CSV, HEADER, NULL '')");
    if (PQresultStatus(res) != PGRES_COPY_IN) {
        fprintf(stderr, "COPY init failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }
    PQclear(res);

    // Stream CSV data
    while (fgets(line, MAX_LINE, fp)) {
        if (PQputCopyData(conn, line, strlen(line)) != 1) {
            fprintf(stderr, "Data send error: %s\n", PQerrorMessage(conn));
            PQfinish(conn);
            return 1;
        }
    }
    
    if (PQputCopyEnd(conn, NULL) != 1) {
        fprintf(stderr, "COPY end error: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }

    // Verify completion
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "COPY failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }

    printf("CSV imported successfully\n");
    
    // Cleanup
    PQclear(res);
    PQfinish(conn);
    fclose(fp);
    return 0;
}
