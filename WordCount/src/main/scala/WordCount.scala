/* WordCount.scala */
import org.apache.spark._
import org.apache.hadoop.fs._

object WordCount {
    def main(args: Array[String]) {
        val files = "/shared/Lab6/wordcount-in/*.*"
        val outputPath = "Lab6/wordcount"
        val conf = new SparkConf().setAppName("WordCount s104062702")
        val sc = new SparkContext(conf)

        // Cleanup output dir
        val hadoopConf = sc.hadoopConfiguration
        val hdfs = FileSystem.get(hadoopConf)
        try { hdfs.delete(new Path(outputPath), true) } catch { case _ : Throwable => { } }

        val lines = sc.textFile(files)
        val counts = lines.flatMap (line => {
            val words = line.split("[^A-Za-z]+").filter(_.nonEmpty).map(word => word.toLowerCase())
            words.map(word => (word, 1))
        }).reduceByKey(_ + _)

        val result = counts.sortBy {
        	case (word, count) => (-count, word)
        } // Sort it

        result.saveAsTextFile(outputPath) // Output
        sc.stop
    }
}
