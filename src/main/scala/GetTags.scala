import Function6.cleanStringa
import org.apache.spark.rdd.RDD

object GetTags {

  def get(): List[String] = {
    val topTags = WebService.dataFrame.rdd
      .map(value => value.getAs[String]("Tags"))
      .map(item => item.split(",")) //Suddivido per virgole
      .flatMap(array => array.map(stringa => cleanStringa(stringa).trim)) //Ripulisco ogni stringa di ogni array
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(15)
      .map { case (tag, _) => tag }

    topTags.toList
  }

}
