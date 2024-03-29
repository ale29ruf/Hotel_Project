import Function6.cleanStringa
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object Function3 {

  // 6) Sulla base dei tag si potrebbero isolare gli hotel più conformi per le esigenze di una persona,
  // sulla base anche della città da visitare o della nazione.
  // Simple recommendation engine to the guest who is fond of a special characteristic of hotel.
  //1) Estrazione 10 top tag
  //2) Associazione hotel -> top 10 tag
  //3) Selezione top 5 tag piu' frequenti per ogni hotel (con relativo hotel che sarebbe la chiave della coppia)

  def eseguiAnalisi(): Array[(String, Seq[String])] = {

    //1) Estrazione 10 top tag
    val topTags: Array[(String, Int)] = WebService.dataFrame.rdd
      .map(value => value.getAs[String]("Tags"))
      .map(item => item.split(",")) //Suddivido per virgole
      .flatMap(array => array.map(stringa => cleanStringa(stringa).trim)) //Ripulisco ogni stringa di ogni array
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(10)

    val hotelTags = WebService.dataFrame.select("Hotel_Address", "Tags")

    val countHotelTags: RDD[(String, Seq[(String, Int)])] = hotelTags.rdd
      .map(row => ( row.getString(0), row.getString(1) ))
      .mapValues { tags => tags
        .split(",") //Suddivido per virgole
        .map(stringa => cleanStringa(stringa).trim) }
      .groupByKey()
      .mapValues( iter => iter
        .flatten
        .groupBy(identity).view.mapValues(_.size)
        .toSeq.sortBy(-_._2)
      )


    //3) Selezione top 5 tag piu' frequenti per ogni hotel (con relativo hotel che sarebbe la chiave della coppia)
    val tagsToExtract: Set[String] = topTags.map(_._1).toSet

    val result: RDD[(String, Seq[String])] = countHotelTags
      .mapValues( seq => seq
        .filter { case (key, _) => tagsToExtract.contains(key) }
        .take(5))
    // rimuovo la frequenza per ogni tag dal momento che sono ordinati già per frequenza
      .map { case (hotel, seq) =>
        val newSeq: Seq[String] = seq.map { case (tag, _) => tag }
        (hotel, newSeq)
      }

    result.collect()
  }

}
