import Helper.Definitions
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, MapReduceBase, Mapper, OutputCollector, Reducer, Reporter, TextInputFormat, TextOutputFormat}

import java.io.IOException
import java.util
import java.util.regex.Pattern
import scala.jdk.CollectionConverters.*
import java.text.SimpleDateFormat
import java.util.Date
import com.typesafe.config.ConfigFactory

object Task1 {
  val definitions = new Definitions()
  val TaskConfig = "Task1"
  val Common = "Common"


  /**Task1 Mapper
   *
   *This Mapper maps the logs into the different types of errors and divide them
   *in time intervals. This time interval is configurable in the application.conf file
   *
   * Output of the mapper is in format : TimeStamp MessageType, 1
   *
   * eg 17:46:00 WARN,1
   */
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()
    private val Thousand = 1000

    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val config = ConfigFactory.load()
      val task_config = config.getConfig(TaskConfig)
      val conf = config.getConfig(definitions.PAT)
      val patternReg = Pattern.compile(conf.getString(definitions.Det_pat))
      val patternMsg = Pattern.compile(conf.getString(definitions.ERR_PAT))
      val matcherMsg = patternMsg.matcher(value.toString)
      val interval = task_config.getString(definitions.Interval).toInt
      val line: String = value.toString
      val arr = line.split(definitions.Blank).toList
      val matcher3 = patternReg.matcher(arr.last)
      if (matcher3.find()) {
        val dateFormatter = new SimpleDateFormat(conf.getString(definitions.TimePatMilliSec))
        val dateFormatterRet = new SimpleDateFormat(conf.getString(definitions.TimePatSec))
        val date = ((dateFormatter.parse(arr.head)).getTime) / Thousand //Dividing the time from 1000 to remove the millisecond from the time
        val d1 = (date.toInt) / interval // Dividing by intervals to make specific time splits
        val d = new Date(d1 *(interval)*Thousand) //Multiplying back with 1000 to get back to the previous millisecond time
        val d2 = dateFormatterRet.format(d)
        if(matcherMsg.find()){
          val msg = matcherMsg.group()
          val msgType = d2 + definitions.Blank + msg //Building the key with TimeStamp + Msg type
          word.set(msgType)
          output.collect(word, one)
        }
      }

  /**Task1 Reducer
   *
   *This Reducer just adds the values from the mapper and gives the count of log types in specified time interval
   *
   */
  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key,  new IntWritable(sum.get()))

  @main def runMapReduce1(inputPath: String, outputPath: String) =
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
