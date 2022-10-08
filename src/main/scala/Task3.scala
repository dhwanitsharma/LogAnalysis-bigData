import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*
import java.util.regex.Pattern

import scala.jdk.CollectionConverters.*
import java.io.IOException
import java.util
import com.typesafe.config.*

import Helper.Definitions
import Helper.CreateLogger

class Task3{}

object Task3 {
  val definitions = new Definitions()
  val TaskConfig = "Task3"
  val Common = "Common"
  val logger = CreateLogger(classOf[Task3])

  /**Task3 Mapper
   *
   *This Mapper maps the logs into the different types of message types.
   *
   * Output of the mapper is in format : MessageType,1
   *
   * eg DEBUG,1
   */
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()

    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val config = ConfigFactory.load()
      val conf = config.getConfig(definitions.PAT)
      val pattern = Pattern.compile(conf.getString(definitions.ERR_PAT)) //Check all the patterns
      val matcher = pattern.matcher(value.toString)
      if (matcher.find()) {
        val msgType = matcher.group()
        word.set(msgType)
        logger.debug("Mapper Output" + msgType+","+one)
        output.collect(word, one)
      }

  /**Task3 Reducer
   *
   *This Reducer just adds the values from the mapper and gives the count of log types
   *
   * Output of the reducer is in format : MessageType,Count
   *
   * Example : DEBUG,7
   *
   */
  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("Task1 Reducer for key :"+ key)
      logger.debug(key.toString, values.toString)
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key,  new IntWritable(sum.get()))

 // @main def runMapReduce(inputPath: String, outputPath: String) =
  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val outputPath = args(1)
    require(!inputPath.isEmpty && !outputPath.isEmpty)
    logger.debug("Input + OutputPath ="+inputPath +"+"+ outputPath)
    val configuration = ConfigFactory.load()
    val task_config = configuration.getConfig(TaskConfig)
    val comm_config = configuration.getConfig(Common)
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName(task_config.getString(definitions.Job_Name))
    //conf.set(comm_config.getString(definitions.HDFS),comm_config.getString(definitions.Path))
    conf.set(comm_config.getString(definitions.Map_Job), task_config.getString(definitions.Map_Cnt))
    conf.set(comm_config.getString(definitions.Red_Job), task_config.getString(definitions.Red_Cnt))
    conf.set(comm_config.getString(definitions.Seperator),definitions.Comma)
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    logger.info("Task3 Job is starting")
    JobClient.runJob(conf)
  }
}
