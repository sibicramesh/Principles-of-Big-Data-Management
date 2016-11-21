
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.util
import org.apache.spark.sql.SQLContext

object wordcount {
  
  def main(args: Array[String]) {
    
    System.setProperty("hadoop.home.dir","C:\\hadoop-2.3.0\\bin\\tweet")
    val conf = new SparkConf().setAppName("CountSpark").setMaster("local[2]").set("spark.executor.memory","8g")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("C:\\hadoop-2.3.0\\bin\\tweet\\text_tweets.txt")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile("C:\\hadoop-2.3.0\\bin\\tweet\\text_tweet.txt")
  }
}