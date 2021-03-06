# pyspark_learning

Learning doc: https://docs.google.com/document/d/1DGQjyAY1re2QO-zejYSdxMhFbQ9NzPo8_SXXZpTdFWw/edit

Problem Statement: We need to be able to import products from a CSV file and into a database (which has 1/2 M records). There are half a million product details to be imported into the database.

Data: https://drive.google.com/file/d/1WES5en6izcJGtZgS4JzPNZyzdQVNecMJ/view?usp=sharing

Points to achieve:
1. Support for regular non-blocking parallel ingestion of the given file into a table. Consider thinking about the scale of what should happen if the file is to be processed in 2 mins.
2. Support for updating existing products in the table based on `sku` as the primary key.
3. All product details are to be ingested into a single table

Solution:
1. Create virtual env and install pyspark.
2. Install JVM and postgres on local
3. For creating table:
     create table pyspark.products (sku varchar(55), name varchar(255), description varchar(1024))
4. Solution steps are mentioned in Jupyter Notebook.
You can run pyspark_learning.ipynb notebook, which will add records in table in an under 10 secs.

Things to do:
1. Partition data and read the partitions in parallel. (https://devblogs.microsoft.com/azure-sql/partitioning-on-spark-fast-loading-clustered-columnstore-index/)
2. Add update (Upsert) logic while adding changed data.
