# SEMS-Spark

## Problems that occurred during dev

### Update on May 17th, 2017
While Jacob found a way to use DataFrames to conduct the regressions, one cannot simply use a map function to conduct the regressions spread out over the cluster. He tried to use sparkContext.parallelize(list_of_potential_snps).map(conductRegressions...) to conduct the regressions on the worker nodes, but it looks like it cannot be done so simply. Once things are parallelized with sparkContext, the workers do not appear to be able to find the dataframe passed to the map function.

Jacob is looking more deeply into Spark ml's Pipelines API, as it looks like it may make conducting the regressions in parallel simpler
