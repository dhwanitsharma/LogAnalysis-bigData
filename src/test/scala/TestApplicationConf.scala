import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.typesafe.config.ConfigFactory
import java.util.regex.Pattern
import Helper.Definitions

class TestApplicationConf extends AnyFlatSpec with Matchers  {

  val config = ConfigFactory.load()
  val definitions = new Definitions()

  it should "Task 1 config should be correct" in{
    val task_config = config.getConfig("Task1")
    val interval = task_config.getInt(definitions.Interval)
    val jobName = task_config.getString(definitions.Job_Name)
    val map_Count = task_config.getInt(definitions.Map_Cnt)
    val Red_Count = task_config.getInt(definitions.Red_Cnt)

    interval shouldBe a [Int]
    jobName shouldBe a [String]
    map_Count shouldBe a [Int]
    Red_Count shouldBe a [Int]
  }

  it should "Task 2 config should be correct" in{
    val task_config = config.getConfig("Task2")
    val interval = task_config.getInt(definitions.Interval)
    val jobName = task_config.getString(definitions.Job_Name)
    val map_Count1 = task_config.getInt(definitions.Map_Cnt)
    val Red_Count1 = task_config.getInt(definitions.Red_Cnt)
    val map_Count2 =task_config.getInt(definitions.Map_Cnt_2)
    val Red_Count2 = task_config.getInt(definitions.Red_Cnt_2)
    val err_msg = task_config.getString(definitions.Error_msg)

    interval shouldBe a [Int]
    jobName shouldBe a [String]
    map_Count1 shouldBe a [Int]
    Red_Count1 shouldBe a [Int]
    map_Count2 shouldBe a [Int]
    Red_Count2 shouldBe a [Int]
    assert(err_msg.equals("(ERROR)"))
  }
  it should "Task 3 config should be correct" in{
    val task_config = config.getConfig("Task3")
    val jobName = task_config.getString(definitions.Job_Name)
    val map_Count = task_config.getInt(definitions.Map_Cnt)
    val Red_Count = task_config.getInt(definitions.Red_Cnt)

    jobName shouldBe a [String]
    map_Count shouldBe a [Int]
    Red_Count shouldBe a [Int]
  }

  it should "Task 4 config should be correct" in{
    val task_config = config.getConfig("Task4")
    val jobName = task_config.getString(definitions.Job_Name)
    val map_Count = task_config.getInt(definitions.Map_Cnt)
    val Red_Count = task_config.getInt(definitions.Red_Cnt)

    jobName shouldBe a [String]
    map_Count shouldBe a [Int]
    Red_Count shouldBe a [Int]

  }

}
