import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import java.io._
import java.util.Locale
import org.apache.tika.language.LanguageIdentifier
import java.util.regex.Matcher
import java.util.regex.Pattern

object Twitterstats
{ 
    var firstTime = true
    var t0: Long = 0
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("twitterLog.txt"), "UTF-8"))
    
    // This function will be called periodically after each 5 seconds to log the output. 
    // Elements of a are of type (lang, totalRetweetsInThatLang, idOfOriginalTweet, text, maxRetweetCount, minRetweetCount)
    def write2Log(a: Array[(String, Long, Long, String, Long, Long)])
    {
        if (firstTime)
        {
            bw.write("Seconds,Language,Language-code,TotalRetweetsInThatLang,IDOfTweet,MaxRetweetCount,MinRetweetCount,RetweetCount,Text\n")
            t0 = System.currentTimeMillis
            firstTime = false
        }
        else
        {
            val seconds = (System.currentTimeMillis - t0) / 1000
            
            if (seconds < 60)
            {
                println("Elapsed time = " + seconds + " seconds. Logging will be started after 60 seconds.")
                return
            }
            
            println("Logging the output to the log file\nElapsed time = " + seconds + " seconds\n-----------------------------------")
            
            for(i <-0 until a.size)
            {
                val langCode = a(i)._1
                val lang = getLangName(langCode)
                val totalRetweetsInThatLang = a(i)._2
                val id = a(i)._3
                val textStr = a(i)._4.replaceAll("\\r|\\n", " ")
                val maxRetweetCount = a(i)._5
                val minRetweetCount = a(i)._6
                val retweetCount = maxRetweetCount - minRetweetCount + 1
                
                bw.write("(" + seconds + ")," + lang + "," + langCode + "," + totalRetweetsInThatLang + "," + id + "," + 
                    maxRetweetCount + "," + minRetweetCount + "," + retweetCount + "," + textStr + "\n")
            }
        }
    }
  
    // Pass the text of the retweet to this function to get the Language (in two letter code form) of that text.
    def getLang(s: String) : String =
    {
        val inputStr = s.replaceFirst("RT", "").replaceAll("@\\p{L}+", "").replaceAll("https?://\\S+\\s?", "")
        var langCode = new LanguageIdentifier(inputStr).getLanguage
        
        // Detect if japanese
        var pat = Pattern.compile("\\p{InHiragana}") 
        var m = pat.matcher(inputStr)
        if (langCode == "lt" && m.find)
            langCode = "ja"
        // Detect if korean
        pat = Pattern.compile("\\p{IsHangul}");
        m = pat.matcher(inputStr)
        if (langCode == "lt" && m.find)
            langCode = "ko"
        
        return langCode
    }
  
    // Gets Language's name from its code
    def getLangName(code: String) : String =
    {
        return new Locale(code).getDisplayLanguage(Locale.ENGLISH)
    }
  
    def main(args: Array[String]) 
    {
        // Configure Twitter credentials
        val apiKey = "IbhJBsekDkQsCDYPaky1H3UGE"
        val apiSecret = "KT07ZoI0KIc8F6R0vKXoW6FBMzAdU3dgQ03tPnINvBJUGvxRn7"
        val accessToken = " 782502963076009984-PwAGgcQwNiMkhtC7PC8cnirZfGXgnq3"
        val accessTokenSecret = "iT7Wowsjbw3k0fzVMYY5k9ZyyMR3wuMyLAyMvUWbHGES4"
        
        Helper.configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret)
        
        val ssc = new StreamingContext(new SparkConf(), Seconds(5))
        val tweets = TwitterUtils.createStream(ssc, None)
                    // Filter the tweets, keep only retweets
                    .filter(x => x.isRetweet())
                    // Get the original tweet of these retweets
                    .map(retweet => retweet.getRetweetedStatus())
                    // Map each tweet to the following form:
                    //         (tweet_id, (retweet_count, retweet_count, text, language_code))
                    // retweet_count appears twice, this is needed for reduceByKeyAndWindow. 
                    .map(x => (x.getId(),(x.getRetweetCount(), x.getRetweetCount(), x.getText(), getLang(x.getText))))
                    // Reduce the tweets by the tweet_id, and compute the max and min of retweet_count 
                    // for each window of 60 seconds every 5 seconds
                    .reduceByKeyAndWindow(  (a:(Long,Long,String,String), b:(Long,Long,String,String)) 
                                                    => 
                                            (Math.max(a._1,b._1), Math.min(a._2,b._2), a._3, a._4), 
                                            Seconds(60), 
                                            Seconds(5))
                    // Make the language_code the key
                    .map{case(id,(max,min,text,lang)) => (lang, (id, max, min, text))}
                    // Persist the results, because it is used twice later
                    // If the results are not persisted, this calculations will be done twice
                    .cache()

        // Compute the total retweet counts per language_code
                                            // Map each tweet to (language_code, retweet_count)
        val retweets_per_language = tweets  .map{case(lang,(id, max, min, text)) => (lang, max-min+1)}
                                            // Compute the total retweet counts per language_code
                                            // for each window of 60 seconds every 5 seconds
                                            .reduceByKeyAndWindow(_ + _, _ - _, Seconds(60), Seconds(5))

        // Join each tweet with the corresponding language 
        // to get the total retweet count for that langage of that tweet
        val outDStream = tweets .join(retweets_per_language)
                                // Map the tweets to the structure that write2Log expects
                                .map{case(lang,((id,max,min,text), tweets_pl)) => (lang, tweets_pl, id, text, max, min)}
                                // Composite key for sorting: (total_retweets_language, language, retweet_count_tweet)
                                // Sort the tweets by the total retweets per language (primary key) and 
                                // than according to the language code 
                                // (because there could be different languages with the same retweet count, 
                                //     and we want to group tweets with from the same language) 
                                // and than the retweetcount per tweet.
                                .transform(rdd => rdd.sortBy( x => (x._2, x._1, x._5 - x._6 + 1), false))
                                // Log the tweets for each rdd
                                .foreachRDD(rdd => write2Log(rdd.collect))
        
        // If elements of RDDs of outDStream aren't of type (lang, totalRetweetsInThatLang, idOfOriginalTweet, text, maxRetweetCount, minRetweetCount),
        //    then you must change the write2Log function accordingly.
    
        new java.io.File("cpdir").mkdirs
        ssc.checkpoint("cpdir")
        ssc.start()
        ssc.awaitTermination()
    }
}

