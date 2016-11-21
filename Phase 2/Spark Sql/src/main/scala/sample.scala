import oauth.signpost.commonshttp.CommonsHttpOAuthConsumer
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext



object sample {
  //Twitter Authentication
  val AccessToken = "1048610250-QQZ8D05FWBIon130QSgjg0XGDN0dw3lXXhP7KFt";
  val AccessSecret = "RRiMG6c7mIY61apEJWSwoxMMaSVN8tQwIcuK627ugp46r";
  val ConsumerKey = "RRAnQIWfiuDBpJm94OWgwmpEF";
  val ConsumerSecret = "uXj3hPKmkU931K8ye5FMZemBUky4UyEQxQCz2Ej5qyS4zp0Ddw";

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\hadoop-2.3.0\\bin\\tweet")
    val conf = new SparkConf().setAppName("CountSpark").setMaster("local[2]").set("spark.sql.warehouse.dir","file:///c:/tmp/spark-warehouse")
    val sc = new SparkContext(conf)
    val sqlcontext = new SQLContext(sc)
    import sqlcontext.implicits._

    //Spark DataFrames
    val tweetsfile = sqlcontext.read.json("C:\\Users\\bn4n5\\workspace\\Pb-ass\\mypackage\\fetched_tweet.json")
    tweetsfile.registerTempTable("querytable1")

    //Spark RDD's
    val string=sc.textFile("C:\\Users\\bn4n5\\workspace\\Pb-ass\\mypackage\\fetched_tweet.json")

    var a='Y'
    while (a=='Y') {
    //Menu Option
    println("****** Analytical Queries using Apache Spark ******")
    println("1=>Top Users who has Tweeted the most times")
    println("2=>Users with Most Sensitive Tweet Numbers")
    println("3=>Top Hashtags used in my collected data in conjunction with Trending Hash tags Topics")
    println("4=>Cities from which most Tweets and Retweets posted")
    println("5=>Most Popular Time Zones")
    println("Enter your choice:")
    val choice=readInt()
      choice match {

        case 1 =>
          //Query 1 using Spark DataFrames
          val Query1 = sqlcontext.sql("select user.name,user.screen_name, count(user.followers_count) as tweetsCount from querytable1 group by user.screen_name,user.name order by tweetsCount desc limit 10")
          Query1.show()

          //Query 1 calling public API
          val name = readLine("Enter screen name to find user IDs for every user following the specified user:")
          val consumer = new CommonsHttpOAuthConsumer(ConsumerKey, ConsumerSecret)
          consumer.setTokenWithSecret(AccessToken, AccessSecret)
          val request = new HttpGet("https://api.twitter.com/1.1/followers/ids.json?cursor=-1&screen_name=" + name)
          consumer.sign(request)
          val client = new DefaultHttpClient()
          val response = client.execute(request)
          println(IOUtils.toString(response.getEntity().getContent()))
          println("Press Y to continue or N to exit:")
          a = readChar()

        case 2 =>
          //Query 2 using Spark DataFrames
          val Query2 = sqlcontext.sql("select user.name,count(user.name) as no_of_sensitive_tweets from querytable1 where possibly_sensitive=true and user.lang='en' group by user.name order by no_of_sensitive_tweets desc limit 10")
          Query2.show()
          println("Press Y to continue or N to exit:")
          a = readChar()

        case 3 =>
          //Query 3 using Spark DataFrames
          //Query 3 uses data in the PopularHahtagsAndTopics.txt file posted on Blackboard in conjunction with my collected data
          val text = sc.textFile("C:\\Users\\bn4n5\\workspace\\Pb-ass\\mypackage\\PopularHahtagsAndTopics.txt").map(_.split("/n")).map(frt => Text(frt(0))).toDF()
          text.registerTempTable("querytable")
          val Query=sqlcontext.sql("select querytable.name from querytable where querytable.name like '%#UFC%' or querytable.name like '%#WWE%' or querytable.name like '%#MMA%' ")
          Query.registerTempTable("querytable3")
          val Query3 = sqlcontext.sql("select querytable3.name,count(querytable1.text) as count from querytable1 join querytable3 on querytable1.text like concat ('%',querytable3.name,'%') group by querytable3.name order by count desc limit 10 ")
          Query3.show();
          println("Press Y to continue or N to exit:")
          a = readChar()

        case 4 =>
          //Query 4 using Spark RDD's
          val Query4=string.flatMap(x =>(x.split(",\""))).filter(line=>line.contains("location")).flatMap(x=>(x.split("location\":"))).filter(x => x!="null").filter(x => x!="").filter(line=>line.contains(",")).map(temp => (temp,1)).reduceByKey(_+_).sortBy(_._2,false).take(10).foreach(println)
          println("Press Y to continue or N to exit:")
          a = readChar()

        case 5 =>
          //Query 5 using Spark RDD's
          val Query5=string.flatMap(x =>(x.split(","))).filter(line=>line.contains("time_zone")).flatMap(x =>(x.split("\"time_zone\":"))).filter(x => x!="null").filter(x => x!="").map(temp => (temp,1)).reduceByKey(_+_).sortBy(_._2,false).take(10).foreach(println)
          println("Press Y to continue or N to exit:")
          a = readChar()


      }
    }
  }
}
case class Text(name: String)
