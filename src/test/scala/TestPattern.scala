import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.typesafe.config.ConfigFactory
import java.util.regex.Pattern
import Helper.Definitions

class TestPattern extends AnyFlatSpec with Matchers {

  val config = ConfigFactory.load()
  val definitions = new Definitions()

  it should "Match the log pattern defined in the configuration file, i matches should be true" in{
    val msg = "17:49:38.621 [scala-execution-context-global-25] INFO  HelperUtils.Parameters$ - vRO,PWE8Pdr`8jI\"45F|'3q}ZMIJ|a3a`p%v7$$'hi,/dQm:&<$ed$\\OcN`E9"
    val arr = msg.split(definitions.Blank).toList
    //conf.getString(definitions.Det_pat) = i
    val patternReg = Pattern.compile("i")
    val matcher = patternReg.matcher(arr.last)
    matcher.find() shouldBe(true)
  }

  it should "Match the log pattern defined in the configuration file,i does not match should be false" in{
    val msg = "17:49:38.621 [scala-execution-context-global-25] INFO  HelperUtils.Parameters$ - vRO,PWE8Pdr`8jI\"45F|'3q}ZMIJ|a3a`p%v7$$'h,/dQm:&<$ed$\\OcN`E9"
    val arr = msg.split(definitions.Blank).toList
    //conf.getString(definitions.Det_pat) = i
    val patternReg = Pattern.compile("i")
    val matcher = patternReg.matcher(arr.last)
    matcher.find() shouldBe(false)
  }

  it should "Detect pattern should be String" in{
    val conf = config.getConfig(definitions.PAT)
    val det_pattern = conf.getString(definitions.Det_pat)
    det_pattern shouldBe a [String]
  }

  it should "Error pattern should be a specific pattern" in{
    val conf = config.getConfig(definitions.PAT)
    val det_pattern = conf.getString(definitions.ERR_PAT)
    assert(det_pattern.equals("(INFO|WARN|DEBUG|ERROR)"))
  }

  it should "Time_Millisecond pattern should be a specific pattern" in{
    val conf = config.getConfig(definitions.PAT)
    val det_pattern = conf.getString(definitions.TimePatMilliSec)
    assert(det_pattern.equals("HH:mm:ss.SSS"))
  }

  it should "Time_second pattern should be a specific pattern" in{
    val conf = config.getConfig(definitions.PAT)
    val det_pattern = conf.getString(definitions.TimePatSec)
    assert(det_pattern.equals("HH:mm:ss"))
  }

}
