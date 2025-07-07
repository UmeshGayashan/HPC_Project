# HPC_Project
Database searching operations

Dataset Link

https://www.kaggle.com/datasets/jillanisofttech/amazon-product-reviews

### To enter the data in CSV file

gcc -o dataEntering dataEntering.c -I/usr/include/postgresql -lpq

./dataEntering Reviews.csv

### To do the serial searching in PostgreSQL database

gcc -o simpleSearch simpleSearch.c -I/usr/include/postgresql -lpq

./simpleSearch "A395BORC6FGVXV" "A1MZYO9TZK0BBI"

### To do the parallel searching in PostgreSQL database

gcc -fopenmp -o parallel_scan OpenMP/Parallelization.c  -I/usr/include/postgresql -lpq

./parallel_scan "A395BORC6FGVXV" "A1MZYO9TZK0BBI"