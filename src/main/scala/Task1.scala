import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, MapReduceBase, Mapper, OutputCollector, Reducer, Reporter, TextInputFormat, TextOutputFormat}

import java.io.IOException
import java.util
import java.util.regex.Pattern
import scala.jdk.CollectionConverters.*
import java.text.SimpleDateFormat
import java.util.Date

object Task1 {
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()
    private val Thousand = 1000

    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val patternTime = Pattern.compile("[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,3})?")
      val patternReg = Pattern.compile("@")
      val patternMsg = Pattern.compile("(INFO|WARN|DEBUG|ERROR)")
      val matcherMsg = patternMsg.matcher(value.toString)
      val interval = 60
      val matcher1 = patternTime.matcher(value.toString)
      val matcher2 = patternReg.matcher(value.toString)
      val line: String = value.toString
      val arr = line.split(" ").toList
      val matcher3 = patternReg.matcher(arr.last)
      if (matcher3.find()) {
        val dateFormatter = new SimpleDateFormat("HH:mm:ss.SSS")
        val dateFormatterRet = new SimpleDateFormat("HH:mm:ss")
        val date = ((dateFormatter.parse(arr.head)).getTime) / 1000
        val d1 = (date.toInt) / interval
        val d = new Date(d1 *(interval)*1000)
        val d2 = dateFormatterRet.format(d)
        if(matcherMsg.find()){
          val msg = matcherMsg.group()
          val msgType = d2 + "," + msg
          word.set(msgType)
          output.collect(word, one)
        }
      }


  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key,  new IntWritable(sum.get()))

  @main def runMapReduce1(inputPath: String, outputPath: String) =
    require(!inputPath.isBlank && !outputPath.isBlank)
    println(inputPath)
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("WordCount")
    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.set("mapred.textoutputformat.separator", ",")
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
