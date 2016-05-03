import org.apache.spark._
import org.apache.hadoop.fs._

object ReachabilityQuery {
    def main(args: Array[String]) {
        val filePath = args(0)
        val name = args(1)
        val iters = args(2).toInt

        val outputPath = "Lab6/reachability"

        val conf = new SparkConf().setAppName("Reachability Query Application")
        val sc = new SparkContext(conf)

        // Cleanup output dir
        val hadoopConf = sc.hadoopConfiguration
        var hdfs = FileSystem.get(hadoopConf)
        try { hdfs.delete(new Path(outputPath), true) } catch { case _ : Throwable => { } }

        // Step 0: Read input file
        val lines = sc.textFile(filePath, sc.defaultParallelism*3)

		// Step 1: initial friend list, map each line in file to pairs
		val friendList = lines.flatMap(line => {
			val ls = line.split(',')
			val ll = (ls(0) -> ls(1))
			List((ll._1->ll._2),(ll._2->ll._1))
		}) 
				// ex: "Tom,Alice" => ("Tom" -> "Alice"), ("Alice" -> "Tom")
				// hint: use map() and union()
				//       Or, you can use flatMap() to do it at once
				// ...

        // Step 2: make an RDD from 'name'
		var res = sc.parallelize(List(name), sc.defaultParallelism*3)

				// hint: use sc.parallelize()
				// optional: you can pass (sc.defaultParallelism * 3) as # of slices for better performance
				//
				// var res = ...

        // Step 3: use initial result and friend list to find all friends within 'iters' hops
		for (i <- 1 to iters) {
			res = res.union(res.map(x => (x,x)).join(friendList).values.map(x => x._2)).distinct()
		}

        // hint: use join(), union() and distinct()
        // optional: resize numPartitions to (sc.defaultParallelism * 3) at the end of each iteration
        //
        // for (i <- 1 to iters ) {
        //     res = res. ...
        // }

        // Action branch! Add cache() to avoid re-computation
        res = res.cache

        // Output: count() and saveAsTextFile()
        println("Count := " + res.count)
        res.sortBy(_.toString).saveAsTextFile(outputPath)

        sc.stop
    }
}

