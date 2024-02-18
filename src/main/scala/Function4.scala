import org.apache.spark.rdd.RDD

//SCORE MEDIO PER NAZIONALITA'

object Function4{
  def eseguiAnalisi: collection.Map[String, Double] = {


    val dati=WebService.dataFrame1

    //Selezione delle coppie nazionalità-score

    val coppieNationalityScore: RDD[(String, Double)] =
      dati.rdd.map(riga => (riga.getAs[String]("Reviewer_Nationality"),
        riga.getAs[Double]("Reviewer_Score")))

    //Conteggio delle recensioni per nazionalità
    val nationalityCount: RDD[(String, Int)] = coppieNationalityScore
      .map { case (chiave, _) => (chiave, 1) }.reduceByKey((a, b) => a + b)

    //Somma degli score delle recensioni per nazionalità
    val nationalitySumScore: RDD[(String, Double)] = coppieNationalityScore
      .reduceByKey((a, b) => a + b)

    //Score medio per nazionalità con ordinamwnto
    val nationalityMeanScore = nationalitySumScore.join(nationalityCount)
      .map { case (chiave, (sum, count)) => (chiave, sum / count) }
      .sortBy(coppia => coppia._2, ascending = true)

    nationalityMeanScore.collectAsMap()
  }

  def getAllNationality: Array[String] = {
    val dati=WebService.dataFrame1
    dati.rdd.map(riga => riga.getAs[String]("Reviewer_Nationality")).distinct()
      .sortBy(identity).filter( x=> x!=" ").collect()
  }
}
