import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._
import java.util.Locale
import org.apache.commons.lang3.StringUtils

object AnalyzeTwitters
{
    // Gets Language's name from its code
    def getLangName(code: String) : String =
    {
        return new Locale(code).getDisplayLanguage(Locale.ENGLISH)
    }
    
    def main(args: Array[String]) 
    {
        val input_file = args(0)
        val conf = new SparkConf().setAppName("AnalyzeTwitters")
        val sc = new SparkContext(conf)
        
        // Comment these two lines if you want more verbose messages from Spark
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        
        val t0 = System.currentTimeMillis
        // Expected input format per line: 
        //    <Secs>0,<Lang>,<Langcode>,<TotalRetweetsInThatLang>,<IDOfTweet>,<MaxRetweetCount>,<MinRetweetCount>,
        //    <RetweetCount>,<Text>
        val input_data = sc.textFile(input_file)

        // Compute the max and min retweet count of each tweet id over all windows
                                            // Map each line to a list of words
        val retweets_per_tweet = input_data .map(x => x.split(","))
                                            // Remove the first line
                                            .filter(x => x(0) != "Seconds")
                                            // Map each record to the following format:
                                            // (<IDofTweet>,(<Lang>,<Langcode>, <MaxRetweetCount>, <MinRetweetCount>, <Text>))
                                            .map(x => (x(4).toLong,(x(1), x(2), x(5).toLong, x(6).toLong, x.drop(8).mkString(","))))
                                            // Reduce the tweets by their id, and compute the max and min of 
                                            // the max and min retweet counts respectively per tweet id.
                                            .reduceByKey((a,b) => (    a._1, a._2, Math.max(a._3, b._3), Math.min(a._4, b._4), a._5))
                                            // Persist the results, because it is used twice later
                                            // If the results are not persisted, this calculations will be done twice
                                            .cache()


        // Compute the total retweet count per language over all windows and convert it into a Map data stucture
        // Broadcast the map, this will cache the read-only variable on each machine.
        // This will not overwhelm the memory of the machines because the number of possible different languages is small
        // The broadcast is done because a map-side join will be done, where this dataset will be accessed a lot 
        val retweets_per_language = sc.broadcast(    retweets_per_tweet
                                                    // Map each record to the following format:
                                                    // (<Lang>, <retweetCount>)
                                                    .map( x => (x._2._1, x._2._3 - x._2._4 + 1))
                                                    // Compute the total retweet count per language over all windows
                                                    .reduceByKey(_ + _)
                                                    // Convert the rdd into a Map, for easy lookup
                                                    .collectAsMap())
                                                    //output: Map(<Lang>, <RetweetCont>))
        
        // Perform a map-side join to add the total retweets per language to each tweet and convert each tweet to a string
                                        // Only considier tweets retweeted more than once
        val outRDD = retweets_per_tweet .filter(x => x._2._3 > x._2._4)
                                        // Perform a map-side join to add the total retweets per language to each tweet
                                        // Ouput format: (<Language>,<Languagecode>,<TotalRetweetsInThatLang>,<IDOfTweet>,<RetweetCount>,<Text>)
                                        .map( x =>     (x._2._1, 
                                                x._2._2, 
                                                retweets_per_language.value.get(x._2._1), 
                                                x._1,
                                                x._2._3 - x._2._4 + 1,
                                                x._2._5) )
                                        // Composite key for sorting: (total_retweets_language, language, retweet_count_tweet)
                                        // Sort the tweets in desending order by the total retweets per language (primary key) and 
                                        // than according to the language code 
                                        // (because there could be different languages with the same retweet count, 
                                        //     and we want to group tweets with from the same language) 
                                        // and than the retweetcount per tweet.
                                        .sortBy(x => (x._3, x._2, x._5), false)
                                        // Convert each tweet into a string
                                        .map(x =>     x._1 + "," + 
                                                    x._2 + "," + 
                                                    x._3.get + "," + 
                                                    x._4 + "," + 
                                                    x._5 + "," + 
                                                    x._6 + "\n")
        //Output per record: <Language>,<Languagecode>,<TotalRetweetsInThatLang>,<IDOfTweet>,<RetweetCount>,<Text>

        // Write output
        val file = "output.txt"
        val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"))
        bw.write("Language,Language-code,TotalRetweetsInThatLang,IDOfTweet,RetweetCount,Text\n")
        outRDD.collect.foreach(x => bw.write(x))
        bw.close
        
        val et = (System.currentTimeMillis - t0) / 1000
        System.err.println("Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
    }
}

