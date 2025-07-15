# HPC_Project
Database searching operations

Dataset Link

https://www.kaggle.com/datasets/jillanisofttech/amazon-product-reviews

### To enter the data in CSV file

gcc -o dataEntering dataEntering.c -I/usr/include/postgresql -lpq

./dataEntering Reviews.csv

### To do the serial searching in PostgreSQL database

gcc -o simpleSearch simpleSearch.c -I/usr/include/postgresql -lpq

./simpleSearch "A395BORC6FGVXV" "A1MZYO9TZK0BBI"  "A1UQRSCLF8GW1T" "A21BT40VZCCYT4" "A3HDKO7OW0QNK4" "A1SP2KVKFXXRU1" "AQLL2R1PPR46X" "A10EHUTGNC4BGP" "A1Z54EM24Y40LL"

### To do the parallel searching in PostgreSQL database

gcc -fopenmp -o parallel_scan OpenMP/Parallelization.c  -I/usr/include/postgresql -lpq

./parallel_scan "A395BORC6FGVXV" "A1MZYO9TZK0BBI" "A1UQRSCLF8GW1T" "A21BT40VZCCYT4" "A3HDKO7OW0QNK4"

### To do the MPI parallel searching in PostgreSQL database

mpicc -I/usr/include/postgresql -o mpi_search MPI/MPISearch.c -lpq

mpirun -np 5 ./mpi_search "A395BORC6FGVXV" "A1MZYO9TZK0BBI" "A1UQRSCLF8GW1T" "A21BT40VZCCYT4" "A3HDKO7OW0QNK4"

### To do the Hybrid MPI+OpenMP parallel searching in PostgreSQL database

mpicc -fopenmp -o hybrid_search Hybrid/HybridSearch.c -I/usr/include/postgresql -lpq

mpirun -np 3 ./hybrid_search "A395BORC6FGVXV" "A1MZYO9TZK0BBI"  "A1UQRSCLF8GW1T" "A21BT40VZCCYT4" "A3HDKO7OW0QNK4" "A1SP2KVKFXXRU1" "AQLL2R1PPR46X" "A10EHUTGNC4BGP" "A1Z54EM24Y40LL"
