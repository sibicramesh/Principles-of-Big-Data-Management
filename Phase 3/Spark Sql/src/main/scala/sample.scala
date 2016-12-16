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
    val conf = new SparkConf().setAppName("CountSpark").setMaster("local[2]").set("spark.sql.warehouse.dir","file:///c:/tmp/spark-warehouse").set("spark.mongodb.output.uri","mongodb://user:user@ds119608.mlab.com:19608/pbdb?replicaSet=rs-ds119608")
    val sc = new SparkContext(conf)
    val sqlcontext = new SQLContext(sc)
    import sqlcontext.implicits._

    val tweetsfile = sqlcontext.read.json("C:\\Users\\bn4n5\\workspace\\Pb-ass\\mypackage\\fetched_tweet.json")
    tweetsfile.registerTempTable("querytable1")
    //val Query1,Query2,Query3,Query4,Query5
    var a='Y'
    while (a=='Y') {
    //Menu Option
    println("****** Analytical Queries using Apache Spark ******")
    println("1=>Top Users who has Tweeted the most times")
    println("2=>Users with Most Sensitive Tweet Numbers")
    println("3=>Top Hashtags used in my collected data in conjunction with Trending Hash tags Topics")
    println("4=>Cities from which most Tweets posted")
    println("5=>Most Popular Time Zones")
    println("Enter your choice:")

    val choice=readInt()
      choice match {

        case 1 =>
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
          //Query Result copied to MongoDB
          Query1.write.option("collection", "query1").mode("overwrite").format("com.mongodb.spark.sql").save()

        case 2 =>
          val Query2 = sqlcontext.sql("select user.name,count(user.name) as no_of_sensitive_tweets from querytable1 where possibly_sensitive=true and user.lang='en' group by user.name order by no_of_sensitive_tweets desc limit 10")
          Query2.show()
          println("Press Y to continue or N to exit:")
          a = readChar()
          //Query Result copied to MongoDB
          Query2.write.option("collection", "query2").mode("overwrite").format("com.mongodb.spark.sql").save()

        case 3 =>
          //Query 3 uses data in the PopularHahtagsAndTopics.txt file posted on Blackboard in conjunction with my collected data
          val text = sc.textFile("C:\\Users\\bn4n5\\workspace\\Pb-ass\\mypackage\\PopularHahtagsAndTopics.txt").map(_.split("/n")).map(frt => Text(frt(0))).toDF()
          text.registerTempTable("querytable")
          val Query=sqlcontext.sql("select querytable.name from querytable where querytable.name like '%#UFC%' or querytable.name like '%#WWE%' or querytable.name like '%#MMA%' ")
          Query.registerTempTable("querytable3")
          val Query3 = sqlcontext.sql("select querytable3.name,count(querytable1.text) as count from querytable1 join querytable3 on querytable1.text like concat ('%',querytable3.name,'%') group by querytable3.name order by count desc limit 10 ")
          Query3.show();
          println("Press Y to continue or N to exit:")
          a = readChar()
          //Query Result copied to MongoDB
          Query3.write.option("collection", "query3").mode("overwrite").format("com.mongodb.spark.sql").save()

        case 4 =>
          val Query4=sqlcontext.sql("select user.location,count(*) as users from querytable1 where user.location like '%,%' and user.location not like '%1%' group by user.location order by users desc limit 10")
          Query4.show()
          println("Press Y to continue or N to exit:")
          a = readChar()
          //Query Result copied to MongoDB
          Query4.write.option("collection", "query4").mode("overwrite").format("com.mongodb.spark.sql").save()

        case 5 =>
          val Query5=sqlcontext.sql("select user.time_zone,count(*) as users from querytable1 where user.time_zone <> 'null' group by user.time_zone order by users desc limit 10")
          Query5.show()
          println("Press Y to continue or N to exit:")
          a = readChar()
          //Query Result copied to MongoDB
          Query5.write.option("collection", "query5").mode("overwrite").format("com.mongodb.spark.sql").save()

      }

    }
  }
}
case class Text(name: String)
