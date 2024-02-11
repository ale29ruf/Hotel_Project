import TagsAssociationAnalysis.cleanStringa
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object Function3 {

  // 6) Sulla base dei tag si potrebbero isolare gli hotel più conformi per le esigenze di una persona,
  // sulla base anche della città da visitare o della nazione.
  // Simple recommendation engine to the guest who is fond of a special characteristic of hotel.
  //1) Estrazione 10 top tag
  //2) Associazione hotel -> top 10 tag
  //3) Selezione top 5 tag piu' frequenti per ogni hotel (con relativo hotel che sarebbe la chiave della coppia)

  def eseguiAnalisi(dataFrame: DataFrame): Unit = {

    //1) Estrazione 10 top tag
    val topTags = dataFrame.rdd
      .map(value => value.getAs[String]("Tags"))
      .map(item => item.split(",")) //Suddivido per virgole
      .flatMap(array => array.map(stringa => cleanStringa(stringa).trim)) //Ripulisco ogni stringa di ogni array
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(10)

    val hotelTags = dataFrame.select("Hotel_Address", "Tags")

    // 2) Associazione hotel -> top 5 tag
    val countHotelTags = hotelTags.rdd
      .map(row => ( row.getString(0), row.getString(1) ))
      .mapValues { tags => tags
        .split(",") //Suddivido per virgole
        .map(stringa => cleanStringa(stringa).trim)
      }
      .groupByKey()
      .mapValues( iter => iter
        .flatten
        .groupBy(identity).view.mapValues(_.size)
        .toSeq.sortBy(-_._2)
      )


    //3) Selezione top 5 tag piu' frequenti per ogni hotel (con relativo hotel che sarebbe la chiave della coppia)
    val tagsToExtract = topTags.map(_._1).toSet

    val result = countHotelTags
      .mapValues( seq => seq
        .filter { case (key, _) => tagsToExtract.contains(key) }
        .take(5))


  }

}
