import breeze.linalg.split
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by harry on 2015/12/18.
  */
object Main {
  def main(args: Array[String]) {
    val inputFile=args(0)
    val outputFile=args(1)

    val conf=new SparkConf().setAppName("userratingrank")
    val sc=new SparkContext(conf)

    val ratingdata=sc.textFile(inputFile).filter(line => !line.contains("user_ID"))
    val ratingdataMap=ratingdata.map(line => {
      val data=line.split(',')
      val userid=data(0).toInt
      val itemid=data(1).toInt
      val rating=data(2).toInt
      if(userid <= 10 && itemid <= 10 && rating >3){
        ("User "+userid+" 評價 Item "+itemid+" 為 "+rating,rating)
      }else{
        null
      }
    }).filter(line => line != null)
    ratingdataMap.sortBy(pair=>pair._2,false).keys.saveAsTextFile(outputFile)

  }
}
