import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._
import java.util.Locale
import org.apache.commons.lang3.StringUtils

object Pagecounts 
{
    // Gets Language's name from its code
    def getLangName(code: String) : String =
    {
        new Locale(code).getDisplayLanguage(Locale.ENGLISH)
    }
    
    def main(args: Array[String]) 
    {
        val input_file = args(0) // Get input file's name from this command line argument
        val conf = new SparkConf().setAppName("Pagecounts")
        val sc = new SparkContext(conf)

        
        // Uncomment these two lines if you want to see a less verbose messages from Spark
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        
        val t0 = System.currentTimeMillis
        //Input format: <Language code> <Page title> <View count> <Page size>
        val input_data = sc.textFile(input_file)

                                    // Map each line to a list of the words
        val outRDD = input_data     .map(x => x.split(" "))
                                    // Filter the lines, remove line with the same language code and page title
                                    .filter(x => StringUtils.substringBefore(x(0),".") != x(1))
                                    // Map the lines to the following format:
                                    // (page_language, (view_count, page_title, view_count))
                                    .map(x => (StringUtils.substringBefore(x(0),".") , (x(2).toLong, x(1), x(2).toLong)))
                                    // Reduce the pages by language, compute the sum of the views per language
                                    // and get the page with the largest view per language
                                    // Output stucture of a record: 
                                    // (<Language>,(<TotalViewsInThatLang>,<MostVisitedPageInThatLang>,<ViewsOfThatPage>))
                                    .reduceByKey((a, b) => 
                                                        (if (a._3 > b._3)     (a._1 + b._1, a._2, a._3) 
                                                         else                 (a._1 + b._1, b._2, b._3)))
                                    // Sort the records with <TotalViewsInThatLang> as key in desending order
                                    .sortBy(x => x._2._1, false)
                                    // Map each record to a string
                                    .map(x =>     getLangName(x._1) + "," +
                                                x._1 + "," +
                                                x._2._1 + "," +
                                                x._2._2 + "," +
                                                x._2._3 + "\n")

        // Write output
        val bw = new BufferedWriter(new OutputStreamWriter(System.out, "UTF-8"))
        bw.write("Language,Language-code,TotalViewsInThatLang,MostVisitedPageInThatLang,ViewsOfThatPage\n")
        outRDD.collect.foreach(x => bw.write(x))
        bw.close
        
        val et = (System.currentTimeMillis - t0) / 1000
        System.err.println("Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
    }
}

