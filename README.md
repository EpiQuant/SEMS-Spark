# SEMS-Spark

## Problems that occurred during dev

### Update on May 17th, 2017
While Jacob found a way to use DataFrames to conduct the regressions, one cannot simply use a map function to conduct the regressions spread out over the cluster. He tried to use sparkContext.parallelize(list_of_potential_snps).map(conductRegressions...) to conduct the regressions on the worker nodes, but it looks like it cannot be done so simply. Once things are parallelized with sparkContext, the workers do not appear to be able to find the dataframe passed to the map function.

Jacob is looking more deeply into Spark ml's Pipelines API, as it looks like it may make conducting the regressions in parallel simpler

## Parsing output
Let's make the output of the parsers be in the following format:

(Vector[(String, Vector[Double])], Vector[String]) where the first entry in the tuple is a collection of key-value associations (tuples really, but spark can treat this as a K-V pair) where the key is the SNP name and the Value is a collection of the SNPs values. The second entry is a collection of the Sample names in the same order as the values in the K-V pair.

It may be best to define a case class instead of dealing with these tuples:

case class ParsedInput(orderedSampleNames: Vector[String], snpInfo: Vector[(String, Vector[Double])])
