/**
 *    Hadoops KeyValueLineRecordReader code is used here as template, 
 *    to convert the lines of the input files to Records of format: (Text(actorName), ArrayWritable(movies))
 */

package recordReader{

import java.io.IOException
import org.apache.hadoop.classification.InterfaceAudience
import org.apache.hadoop.classification.InterfaceStability
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import scala.collection.JavaConversions._

import org.apache.hadoop.mapreduce.lib.input.LineRecordReader
import org.apache.hadoop.io.ArrayWritable
import scala.util.control._



@InterfaceAudience.Public
@InterfaceStability.Stable
class KeyValueLineRecordReader(conf: Configuration) extends RecordReader[Text, ArrayWritable] {

  private val lineRecordReader = new LineRecordReader(Array('\n'.toByte,'\n'.toByte))

  private var key: Text = _

  private var value: ArrayWritable = _

  def getKeyClass(): Class[_] = classOf[Text]

  /** 
   * If the input line contains a tv series and/or a movie before 2011, a empty string will be returned.
   * Else the movie name is returned without the leading tabs
   */
  def getMovie( line: String): String = {
      val temp = removeLeadingString("\t", line).split("\\(")
      val title = temp(0)
      var year:Int = 0

      try {
        year = temp(1).substring(0,4).toInt
        if (year < 2011){
          return ""
        }
      } catch {
        case _: Throwable => return ""
      }

      // If the movie name starts with ", return a empty string
      if (temp(0).startsWith("\"")) {
        return ""
      } 
      return title + "(" + year.toString + ")"
  }

  /**
   * This function removes the $prefix patterns from the beginning of the given $line
   */
  def removeLeadingString(prefix: String, line: String): String = {
    var result = line;
    while(result.startsWith(prefix)){
      result = result.substring(prefix.length)
      } 
    return result;
  }


  def initialize(genericSplit: InputSplit, context: TaskAttemptContext) {
    lineRecordReader.initialize(genericSplit, context)
  }

  /**
   * During retrival of records spark first calls this function, than it calls getCurrentKey() and getCurrentValue()
   * This function has to make the next key and value ready
   * If the next record is available true is returned
   * If no records are available anymore (end of file), false is returned
   */
  def nextKeyValue(): Boolean = {
    synchronized {
      var movies: List[String] = List()
      var line: String = ""

      // First verify that the end of the file is not yet reached
      if (lineRecordReader.nextKeyValue()) {
          // Get the string of the next record
          // The whole string with the actor and its movies is returned because
          // 2 newlines are set as record delimiters in the constructor of lineRecordReader
        line = lineRecordReader.getCurrentValue.toString

        // Split on the first tab and retrieve the actor name
        var temp = line.split("\t", 2)
        key = new Text(temp(0))

        // Split the rest of the string on a newline
        // This creates a list of the movies, but they still have to be refined
        var movies_raw = temp(1).split("\n")
        var movie = ""
        for( m <- movies_raw ){
            // Get the movie title
            // getMovie returns a empty string if: 
            // $m was not parsable, $m is a tv serie or $m is movie before 2011
            movie = getMovie(m)
            if (! movie.equals("")){
                // Add this movie title to the $movies list
                movies =  movie :: movies
            }
        }
      } else {
          //
        return false
      }
      // If the actor only plays in tv series and/or in movies before 2011, this actor will not be added
      if (movies.isEmpty){
        return nextKeyValue()
      }

      // Convert the list to ArrayWritable
      // because the type of the key and values have to implement the Writable interface
      value = new ArrayWritable(movies.toArray)
      return true
    }
  }

  // After calling nextKeyValue() spark will call getCurrentKey() and getCurrentValue()
  def getCurrentKey(): Text = key

  // After calling nextKeyValue() spark will call getCurrentKey() and getCurrentValue()
  def getCurrentValue(): ArrayWritable = value

  def getProgress(): Float = lineRecordReader.getProgress

  def close() {
    synchronized {
      lineRecordReader.close()
    }
  }
}
}
