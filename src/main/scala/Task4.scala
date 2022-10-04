import Helper.Definitions
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, MapReduceBase, Mapper, OutputCollector, Reducer, Reporter, TextInputFormat, TextOutputFormat}

import java.io.IOException
import java.util
import java.util.regex.Pattern
import scala.jdk.CollectionConverters.*
import java.text.SimpleDateFormat
import java.util.Date

object Task4 {
  val definitions = new Definitions()
  val TaskConfig = "Task4"
  val Common = "Common"

  /**Task4 Mapper
   *
   *This Mapper maps the logs which have the specified pattern into the
   * different types of message types as keys and keeps the String length
   * of the message as the value
   *
   * Output of the mapper is in format : MessageType, Len(Error Message)
   *
   * eg DEBUG,87
   */
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()

    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val config = ConfigFactory.load()
      val conf = config.getConfig(definitions.PAT)
      val patternReg = Pattern.compile(conf.getString(definitions.Det_pat))
      val patternMsg = Pattern.compile(conf.getString(definitions.ERR_PAT))//Pattern to recognize the specific pattern in logger
      val matcherMsg = patternMsg.matcher(value.toString)
      val line: String = value.toString
      val arr = line.split(definitions.Blank).toList
      val msq = arr.last
      val lengthMsg = msq.length()
      val matcher3 = patternReg.matcher(arr.last)
      val lengthMsg1 = new IntWritable(lengthMsg)
      if (matcher3.find()) {
        if(matcherMsg.find()){
          val msg = matcherMsg.group()
          val msgType = msg
          word.set(msgType)
          output.collect(word, lengthMsg1)
        }
      }

  /**Task4 Reducer
   *
   *This Reducer finds the maximum value for the error message length provided by the mapper
   *
   * Output of the reducer is in format : MessageType, MaxLen(Error Message)
   *
   * Example : DEBUG,68
   *
   */
  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      //Using Math.max to find the max value
      val max = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(Math.max(valueOne.get(),valueTwo.get())))
      output.collect(key,  new IntWritable(max.get()))

  @main def runMapReduce2(inputPath: String, outputPath: String) =
    require(!inputPath.isBlank && !outputPath.isBlank)
    println(inputPath)
    val configuration = ConfigFactory.load()
    val task_config = configuration.getConfig(TaskConfig)
    val comm_config = configuration.getConfig(Common)
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName(task_config.getString(definitions.Job_Name))
    conf.set(comm_config.getString(definitions.HDFS),comm_config.getString(definitions.Path))
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
    JobClient.runJob(conf)
}
