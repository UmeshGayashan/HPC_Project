# HPC_Project
Database searching operations

Dataset Link

https://www.kaggle.com/datasets/jillanisofttech/amazon-product-reviews

### To enter the data in CSV file

gcc -o dataEntering dataEntering.c -I/usr/include/postgresql -lpq

./dataEntering Reviews.csv

### To do the serial searching in PostgreSQL database

gcc -o simpleSearch simpleSearch.c -I/usr/include/postgresql -lpq

./simpleSearch

