/**
 *	Hadoops KeyValueLineInputFormat code is used here as template
 */

package inputFormat{

import java.io.IOException
import org.apache.hadoop.classification.InterfaceAudience
import org.apache.hadoop.classification.InterfaceStability
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.ArrayWritable

import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.compress.SplittableCompressionCodec
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import scala.collection.JavaConversions._

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import recordReader._
 

@InterfaceAudience.Public
@InterfaceStability.Stable
class KeyValueTextInputFormat extends FileInputFormat[Text, ArrayWritable] {

  protected override def isSplitable(context: JobContext, file: Path): Boolean = {
    val codec = new CompressionCodecFactory(context.getConfiguration)
      .getCodec(file)
    if (null == codec) {
      return true
    }
    codec.isInstanceOf[SplittableCompressionCodec]
  }

  def createRecordReader(genericSplit: InputSplit, context: TaskAttemptContext): RecordReader[Text, ArrayWritable] = {
    context.setStatus(genericSplit.toString)
    new KeyValueLineRecordReader(context.getConfiguration)
  }
}
}
