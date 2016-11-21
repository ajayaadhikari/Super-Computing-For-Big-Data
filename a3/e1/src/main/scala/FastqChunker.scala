
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat 
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}

import java.io.BufferedWriter 
import java.io.File
import java.io.OutputStreamWriter 
import java.util.zip.GZIPOutputStream

import java.io._
import java.nio.file.{Paths, Files}

object FastqChunker 
{
	val nrPartitions = 8
def main(args: Array[String]) 
{
	if (args.size < 3)
	{
		println("Not enough arguments!\nArg1 = number of parallel tasks = number of chunks\nArg2 = input folder\nArg3 = output folder")
		System.exit(1)
	}
	
	val prllTasks = args(0)
	val inputFolder = args(1)
	val outputFolder = args(2)

	val file1 = inputFolder + "/fastq1.fq"
	val file2 = inputFolder + "/fastq2.fq"
	
	if (!Files.exists(Paths.get(inputFolder)))
	{
		println("Input folder " + inputFolder + " doesn't exist!")
		System.exit(1)
	}
		 
	// Create output folder if it doesn't already exist
	new File(outputFolder).mkdirs
	
	println("Number of parallel tasks = number of chunks = " + prllTasks + "\nInput folder = " + inputFolder + "\nOutput folder = " + outputFolder)
	
	val conf = new SparkConf().setAppName("DNASeqAnalyzer")
	conf.setMaster("local[" + prllTasks + "]")
	conf.set("spark.cores.max", prllTasks)
	
	val sc = new SparkContext(conf)
	
	// Comment these two lines if you want to see more verbose messages from Spark
	Logger.getLogger("org").setLevel(Level.OFF);
	Logger.getLogger("akka").setLevel(Level.OFF);
		
	var t0 = System.currentTimeMillis
	
	
	// Convert each line of $file1 to records
	val input_rdd1 = sc .textFile(file1)
						// Get the position of each record
						.zipWithIndex()
						// Number each read with it's position
						.map{case (line, index) => (index/4, line)}
						// Group the lines of each read
						.groupByKey()
	// Do the same for $file2
	val input_rdd2 = sc .textFile(file2)
						.zipWithIndex()
						.map{case (line, index) => (index/4, line)}
						.groupByKey()
				// Zip $input_rdd1 with $input_rdd2
	val merged = input_rdd1.zip(input_rdd2)	.map{	case ((index,lines1), (_,lines2)) => 
													(index, lines1.mkString("\n") + "\n" + lines2.mkString("\n"))}
											// Repartition the records equally amongst $prllTasks partiotions
											.repartition(prllTasks.toInt)
	// Write the reads of each partitions to different files
	val rdd = merged.foreachPartition{ iter =>
		var writer:BufferedWriter = null;
		try{
			val firstRead = iter.next;
    		val file:File =  new File(outputFolder + "/output" + firstRead._1 + ".fq.gz");
    		val zip:GZIPOutputStream = new GZIPOutputStream(new FileOutputStream(file));

    		writer = new BufferedWriter(new OutputStreamWriter(zip, "UTF-8"));
    		writer.append(firstRead._2 + "\n");
			while(iter.hasNext){
		        val read = iter.next._2;
		        writer.append(read);
		        if (iter.hasNext){
		        	writer.append("\n");
		        }
		    }
		} finally{           
		    if(writer != null){
			    writer.close();
			    }
		  	}
		}
	
	val et = (System.currentTimeMillis - t0) / 1000 
	println("|Execution time: %d mins %d secs|".format(et/60, et%60))
}
//////////////////////////////////////////////////////////////////////////////
} // End of Class definition
