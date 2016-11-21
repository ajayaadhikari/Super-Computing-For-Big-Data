/* Bacon.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.scheduler.SparkListener 
import org.apache.spark.scheduler.SparkListenerStageCompleted

import java.io._
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce._
import inputFormat._
import org.apache.hadoop.io.ArrayWritable
import java.util.Arrays
import org.apache.spark.storage.StorageLevel



//import bacon.KeyValueLineRecordReader
//import bacon.KeyValueTextInputFormat
///data/sbd2016/lab2input
//ajayadhikari@kova-01.ewi.tudelft.nl

object Bacon 
{
    final val KevinBacon = "Bacon, Kevin (I)"
    val compressRDDs = false
    val logFileName = "sparkLogNotCompressed.txt"
    val resultFileName = "resultNotCompressed.txt"
    // SparkListener must log its output in file sparkLog.txt
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logFileName), "UTF-8"))

    val result = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(resultFileName), "UTF-8"))
    
    def getTimeStamp() : String =
    {
        return "[" + new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime()) + "] "
    }

    /**
     *    The results are saved in this class. 
     *    The toString functions provides a convenient way to return a string in the output format
     */
    object statistics 
    {
        var total_number_males:Long = 0
        var total_number_females:Long = 0
        var total_number_movies:Long = 0
        var distances_males:Array[Long] = new Array[Long](6)
        var distances_females:Array[Long] = new Array[Long](6)

        override def toString(): String =
        {
            val total_actors:Float = total_number_males + total_number_females;
            val male_per = total_number_males/total_actors*100;
            val female_per = total_number_females/total_actors*100;
            var result =     f"Total number of actors = $total_actors, out of which $total_number_males (${male_per}%1.1f%%) are male while " +
                            f"$total_number_females (${female_per}%1.1f%%) are females.\n" +
                            f"Total number of movies = $total_number_movies\n\n";

            for(i <- 1 to 6 ){
                val actors = distances_males(i-1);
                val actresses = distances_females(i-1);
                val actors_per = actors/total_number_males.toFloat*100;
                val actresses_per = actresses/total_number_females.toFloat*100;
                result += f"There are $actors actors (${actors_per}%1.1f%%) and $actresses actresses (${actresses_per}%1.1f%%) at distance $i\n";
            }
            val total_males_1_6 = distances_males.reduce(_ + _)
            val total_female_1_6 = distances_females.reduce(_ + _)
            val total_1_6 = total_males_1_6 + total_female_1_6
            val male_per_1_6 = total_males_1_6/total_number_males.toFloat*100
            val female_per_1_6 = total_female_1_6/total_number_females.toFloat*100
            val total_per_1_6 = total_1_6/total_actors
            result += f"\nTotal number of actors from distance 1 to 6 = $total_1_6, ratio = $total_per_1_6\n" + 
                        f"Total number of male actors from distance 1 to 6 = $total_males_1_6, ratio = $male_per_1_6\n" + 
                        f"Total number of female actors (actresses) from distance 1 to 6 = $total_female_1_6, ratio = $female_per_1_6\n"
            return result;
        }
    }

    def main(args: Array[String]) 
    {
        val cores = args(0)             // Number of cores to use
        val inputFileM = args(1)        // Path of input file for male actors
        val inputFileF = args(2)        // Path of input file for female actors (actresses)
        

        val conf = new SparkConf().setAppName("Kevin Bacon app")
        conf.setMaster("local[" + cores + "]")
        conf.set("spark.cores.max", cores)
        conf.set("KEY_VALUE_SEPERATOR", "\t")
        conf.set("spark.rdd.compress", compressRDDs.toString)
        val sc = new SparkContext(conf)

        val MALE_TAG = true
        val FEMALE_TAG = false
        
        println("Number of cores: " + args(0))
        println("Input files: " + inputFileM + " and " + inputFileF)
        
        // Comment these two lines if you want to see more verbose messages from Spark
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        sc.addSparkListener(new SparkListener() {
            override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) 
            {
                val rdds = stageCompleted.stageInfo.rddInfos
                rdds.foreach(rdd => {
                        bw.write(f"\nrdd name: ${rdd.name}: ${getTimeStamp()}\n" + 
                                f"\trdd diskSize = ${rdd.diskSize}\n" +
                                f"\tmemSize = ${(rdd.memSize / 1000000)}MB\n" + 
                                f"\tnumPartitions = ${rdd.numPartitions}\n" +
                                f"\tnumCachedPartitions = ${rdd.numCachedPartitions}\n")
                })
            }
        });

        /**
         *    This class represents an actor with a name and sex.
         *    Equals and hasCode functions are defined because spark needs this functions
         *    for groupByKey() or reduceByKey() on the key
         */
        class Actor(val name: String, val sex: Boolean) extends Serializable {
              override def equals(that: Any): Boolean =
                that match {
                  case that: Actor => this.hashCode == that.hashCode
                  case _ => false
               }

              override def hashCode:Int = {
                var result = 31
                result = result + (if (name == null) 0 else name.hashCode)
                return result
              }
        }
        
        var t0 = System.currentTimeMillis
        /**
         *    Get 2 rdds one for actors and other actresses. 
         *    The newAPIHadoopFile function returns a rdd with the following format for each record:
         *    <actor, ArrayWritable<movies>>
         *    The tv series and movies created before 2011 are already removed, during the record reading process.
         *    Actors without movies after 2011 are also already removed.
         *    Persist these rdds because they are used more than once.
         *         Storage level MEMORY_ONLY_SER: Store RDD as serialized Java objects (one byte array per partition). 
         *         This is generally more space-efficient than deserialized objects, but more CPU-intensive to read.
         *    The format of the records of the resulting rdds: (Actor(name,sex), Array<movies>)
         */
        val male =      sc .newAPIHadoopFile(inputFileM, classOf[KeyValueTextInputFormat],classOf[Text], classOf[ArrayWritable])
                        .map{case (actor,movies) => (new Actor(actor.toString(), true), movies.toStrings())}
                        .setName("Male Actors")
                        .persist(StorageLevel.MEMORY_ONLY_SER)
                        
        val female = sc .newAPIHadoopFile(inputFileF, classOf[KeyValueTextInputFormat],classOf[Text], classOf[ArrayWritable])
                        .map{case (actress,movies) => (new Actor(actress.toString(), false), movies.toStrings())}
                        .setName("Female Actors")
                        .persist(StorageLevel.MEMORY_ONLY_SER)
                        

        // Save the number of actors and actresses
        this.statistics.total_number_males = male.count()
        this.statistics.total_number_females = female.count()
        println("Number of males actors: " + this.statistics.total_number_males)
        println("Number of females actors: " + this.statistics.total_number_females)

        // Merge the male and female actors in one rdd
        val m_and_f =    male.union(female)

        // Expand each record to possibly multiple records with format (Actor, movie)
        val movies = m_and_f.flatMap{case (actor, movies) => movies.map(movie => (movie, Set(actor)))}
                            .reduceByKey(_ ++ _)
                            .setName("(Actor,movie) records")
                            .persist(StorageLevel.MEMORY_ONLY_SER)
                            

        // Remove the male and female rdds from memory, because it is not used anymore
        male.unpersist()
        female.unpersist()
        this.statistics.total_number_movies = movies.count()
        println("Number of movies: " + this.statistics.total_number_movies)

        /** 
         *    First create a rdd with collaborating actors as records with format (actor1, Set(actor2))
         *    Reduce by key to get all of the collaborating actors per actor: (actor, Set(collaborating actors))
         *    A set is used to remove duplicates
         *    Cache the resulting rdd because it will be used more than once
         */
        val col_actors = movies .flatMap{case (movie, actors) => actors.flatMap(actor1 => actors.map(actor2 => (actor1, Set(actor2)))
                                                                                                .filter(x => !x._2(actor1)))}
                                .reduceByKey(_ ++ _)
                                .setName("(actor, Set(collaborating actors)) records")
                                .persist(StorageLevel.MEMORY_ONLY_SER)
                                  
        // Removed the movies rdd from memory because it is no longer needed
        movies.unpersist()

        /**
         *    Create a new RDD describing actors and their distances to Kevin Bacon (actor, distance),
         *    where distance=100, except for Kevin Bacon for whom distance is 0.
         */
        var distance = col_actors.map{ case (actor:Actor, _) => if(actor.name == KevinBacon) (actor, 0) else (actor, 100)}

        /**
         *    First join distance rdd (actor1, distance) with col_actors (actor1, Set(actor2,actor3,...))
         *    Format per record: (actor1, (distance, Set(actor2, actor3, ..)))
         *    Expand each record to possibly multiple records with format: (actor2, distance + 1)
         *    Reduce by key at the end to get the minimum distance per actor to Bacon with format: (actor, minimum distance)
         *    For example after the first iteration Bacon will have a distance of 0 and every
         *    actor who worked with him would be at distance 1, while the rest of the actors would be at
         *    distance of 100.
         */
        for(x <- 1 to 6 ){
            distance = distance.join(col_actors).flatMap{ case (actor1, (distance, actors)) 
                                                            => Set((actor1,distance)) ++ actors.map(actor2 => (actor2, distance + 1))}
                                                .reduceByKey(Math.min)
        }

        /**
         *     Remove all actors with distance greater than 6.
         *    Cache this rdd because it will be used more than once
         */
        val distances_1_6 = distance.filter{case (_, distance) => distance <= 6 && distance > 0}
                                    .setName("distance 1-6 actors")
                                    .persist(StorageLevel.MEMORY_ONLY_SER)

        /**
         *    Get rdds with only males and female actors
         *    Cache these rdds because they will be used more than once
         */
        val male_1_6 = distances_1_6.filter{case (actor,_) => actor.sex}
                                    .setName("distance 1-6 male actors")
                                    .persist(StorageLevel.MEMORY_ONLY_SER)
        val female_1_6 = distances_1_6  .filter{case (actor,_) => !actor.sex}
                                        .setName("distance 1-6 female actors")
                                        .persist(StorageLevel.MEMORY_ONLY_SER)
        // This rdd is no longer needed
        distances_1_6.unpersist()

        /**
         *    Retrieve the number of male and female actors for each distance in range 1-6.
         *    Collect the male and female actors at distance 6, to output them
         */    
        var male_actors_6 = "\nList of male actors at distance 6:\n"
        var female_actors_6 = "\nList of female actors (actresses) at distance 6:\n"
        for(x <- 1 to 6){
            // If the distance is 6, collect the result and convert them to string
            if (x == 6){
                val male_6 = male_1_6   .filter{case (_, distance) => distance == x}
                                        .sortBy{case (actor, _) => actor.name}
                                        .collect()
                for( i <- 1 to male_6.size)
                    male_actors_6 += f"${i.toString}. ${male_6(i-1)._1.name}\n"

                val female_6 = female_1_6    .filter{case (_, distance) => distance == x}
                                            .sortBy{case (actor, _) => actor.name}
                                            .collect()
                for( i <- 1 to female_6.size)
                    female_actors_6 += f"${i.toString}. ${female_6(i-1)._1.name}\n"
                
                this.statistics.distances_males(5) = male_6.size
                this.statistics.distances_females(5) = female_6.size
            }
            else {
                this.statistics.distances_males(x-1) = male_1_6 .filter{case (_, distance) => distance == x}
                                                                .count()
                this.statistics.distances_females(x-1) = female_1_6 .filter{case (_, distance) => distance == x}
                                                                    .count()
            }
        }

        result.write(statistics.toString())
        result.write(male_actors_6)
        result.write(female_actors_6)
        println("Results are written to file");
        
        sc.stop()
        bw.close()
        
        val et = (System.currentTimeMillis - t0) / 1000 
        println("{Time taken = %d mins %d secs}".format(et/60, et%60))
    }
}
