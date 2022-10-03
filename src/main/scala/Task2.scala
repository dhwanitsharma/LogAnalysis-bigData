import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.regex.Pattern
import scala.jdk.CollectionConverters.*


object Task2 {
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()

    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val patternMsg = Pattern.compile("(INFO)")
      val patternReg = Pattern.compile("i")
      val interval = 60
      val matcherMsg = patternMsg.matcher(value.toString)
      val line: String = value.toString
      val arr = line.split(" ").toList
      val matcher3 = patternReg.matcher(arr.last)
      if (matcher3.find()) {
        val dateFormatter = new SimpleDateFormat("HH:mm:ss.SSS")
        val date = ((dateFormatter.parse(arr.head)).getTime) / 1000
        val d1 = (date.toInt) / interval
        val d = new Date(d1 *(interval)*1000)
        val d2 = dateFormatter.format(d)
        if(matcherMsg.find()){
          val msg = matcherMsg.group()
          val msgType = d2 + "," + msg
          word.set(msgType)
          output.collect(word, one)
        }
      }

  class Map2 extends MapReduceBase with Mapper[LongWritable, Text, Text, Text]:
    private val inputWord1 = new Text()
    private val inputWord2 = new Text()
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, Text], reporter: Reporter): Unit =
      //val inputVal = key.toString
      //val inputKey = value.toString.toInt
      val l = value.toString.split(",").toList
      l.dropRight(1)
      val last = l.last + "," + l.head
      val first = l.take(2)
      inputWord1.set(last)
      inputWord2.set("INFO")
      output.collect(inputWord1, inputWord2)




  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    private final val one = 1
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key,  new IntWritable(sum.get()))

  class Reduce2 extends MapReduceBase with Reducer[Text, Text, Text, Text]:
    private val outputWord = new Text()
    override def reduce(key: Text, values: util.Iterator[Text], output: OutputCollector[Text, Text], reporter: Reporter): Unit =
      val a = key.toString.split(",").toList
      val key1 = a.head

      outputWord.set("INFO")
      output.collect(key,outputWord)

  class DescendingComparator extends WritableComparator(classOf[Text],true){
    override def compare(w1: WritableComparable[_], w2: WritableComparable[_]): Int = {
      val key1 = w1.toString.split(",").toList
      val key = key1.head.toInt
      val key2 = w2.toString.split(",").toList
      val key21 = key2.head.toInt
      if(key <= key21){
        return 1
      }
      else return -1
    }
  }



  @main def runMapReduce3(inputPath: String, outputPath: String,inputPath1: String,  outputPath2: String) =
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
    //JobClient.runJob(conf)
    if(JobClient.runJob(conf).isComplete.equals(true)){
      val conf1: JobConf = new JobConf(this.getClass)
      conf1.setJobName("WordCount")
      conf1.set("fs.defaultFS", "local")
      conf1.set("mapreduce.job.maps", "1")
      conf1.set("mapreduce.job.reduces", "1")
      conf1.set("mapred.textoutputformat.separator", ",")
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
