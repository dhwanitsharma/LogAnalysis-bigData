import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.regex.Pattern
import scala.jdk.CollectionConverters.*
import com.typesafe.config.ConfigFactory
import Helper.Definitions

/**Task2 Mapper 1
 *
 *This Mapper maps the ERROR message type logs which have the
 * specified pattern into the specific time interval and the value is passed as 1.
 *
 * Output of the mapper is in format : TimeStamp, 1
 *
 * eg 17:48:00,1
 */
object Task2 {
  val definitions = new Definitions()
  val TaskConfig = "Task2"
  val Common = "Common"
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()
    private val Thousand = 1000

    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val config = ConfigFactory.load()
      val task_config = config.getConfig(TaskConfig)
      val conf = config.getConfig(definitions.PAT)
      val patternMsg = Pattern.compile(task_config.getString(definitions.Error_msg))
      val patternReg = Pattern.compile(conf.getString(definitions.Det_pat))
      val interval = task_config.getString(definitions.Interval).toInt
      val matcherMsg = patternMsg.matcher(value.toString)
      val line: String = value.toString
      val arr = line.split(definitions.Blank).toList
      val matcher3 = patternReg.matcher(arr.last)
      if (matcher3.find()) {
        val dateFormatter = new SimpleDateFormat(conf.getString(definitions.TimePatSec))
        val date = ((dateFormatter.parse(arr.head)).getTime) / Thousand
        val d1 = (date.toInt) / interval
        val d = new Date(d1 *(interval)*Thousand)
        val d2 = dateFormatter.format(d)
        if(matcherMsg.find()){
          val msg = matcherMsg.group()
          val msgType = d2
          word.set(msgType)
          output.collect(word, one)
        }
      }

  /**Task2 Mapper 2
   *
   *This Mapper maps the output of reducer 1 in where keys and values are interchanged
   *
   * Output of the mapper is in format : Count(Messages), TimeStamp
   *
   * eg 23,17:48:00
   */
  class Map2 extends MapReduceBase with Mapper[LongWritable, Text, Text, Text]:
    private val inputWord1 = new Text()
    private val inputWord2 = new Text()
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, Text], reporter: Reporter): Unit =
      val l = value.toString.split(definitions.Comma).toList
      inputWord1.set(l.last)
      inputWord2.set(l.head)
      output.collect(inputWord1, inputWord2)


  /**Task4 Reducer 1
   *
   *This Reducer adds the value of Mapper 1 and finds the total value for each timeStamp.
   *
   * Output of the reducer is in format : TimeStamp, Count(Messages)
   *
   * Example : 17:48:00,45
   *
   */
  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    private final val one = 1
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key,  new IntWritable(sum.get()))


  /**Task4 Reducer 2
   *
   *This Reducer just formats the output. The compartor is used to change the sequece of the output
   *
   * Output of the reducer is in format : TimeStamp, Count(Messages)
   *
   * Example : 23,17:48:00
   *
   */
  class Reduce2 extends MapReduceBase with Reducer[Text, Text, Text, Text]:
    private val outputWord = new Text()
    override def reduce(key: Text, values: util.Iterator[Text], output: OutputCollector[Text, Text], reporter: Reporter): Unit =
      val val1 = values.asScala.toList.head
      outputWord.set(val1)
      output.collect(key,outputWord)


  /**Task4 Compartor
   *
   * This comparator compares the count of messages in each time interval
   * and sorts them out in descending order on the basis of the count
   *
   */
  class DescendingComparator extends WritableComparator(classOf[Text],true){
    override def compare(w1: WritableComparable[_], w2: WritableComparable[_]): Int = {
      if(w1.toString.toInt <= w2.toString.toInt){
        return 1
      }
      else return -1
    }
  }



  @main def runMapReduce3(inputPath: String, outputPath: String,inputPath1: String,  outputPath2: String) =
    require(!inputPath.isBlank && !outputPath.isBlank)
    println(inputPath)
    val conf: JobConf = new JobConf(this.getClass)
    val configuration = ConfigFactory.load()
    val task_config = configuration.getConfig(TaskConfig)
    val comm_config = configuration.getConfig(Common)
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
    //JobClient.runJob(conf)
    if(JobClient.runJob(conf).isComplete.equals(true)){
      val conf1: JobConf = new JobConf(this.getClass)
      conf1.setJobName(task_config.getString(definitions.Job_Name))
      conf1.set(comm_config.getString(definitions.HDFS),comm_config.getString(definitions.Path))
      conf1.set(comm_config.getString(definitions.Map_Job), task_config.getString(definitions.Map_Cnt_2))
      conf1.set(comm_config.getString(definitions.Red_Job), task_config.getString(definitions.Red_Cnt_2))
      conf1.set(comm_config.getString(definitions.Seperator),definitions.Comma)
      conf1.setOutputKeyComparatorClass(classOf[DescendingComparator])
      conf1.setOutputKeyClass(classOf[Text])
      conf1.setOutputValueClass(classOf[Text])
      conf1.setMapperClass(classOf[Map2])
      conf1.setCombinerClass(classOf[Reduce2])
      conf1.setReducerClass(classOf[Reduce2])
      conf1.setInputFormat(classOf[TextInputFormat])
      conf1.setOutputFormat(classOf[TextOutputFormat[Text, Text]])
      FileInputFormat.setInputPaths(conf1, new Path(inputPath1))
      FileOutputFormat.setOutputPath(conf1, new Path(outputPath2))
      JobClient.runJob(conf1)
    }

}
