import org.apache.spark.rdd.RDD

//SCORE MEDIO PER NAZIONALITA'



object Function4{
  def eseguiAnalisi: collection.Map[String, Double] = {
    val dati=WebService.dataFrame
    val coppieNationalityScore: RDD[(String, Double)] =
      dati.rdd.map(riga => (riga.getAs[String]("Reviewer_Nationality"),
        riga.getAs[Double]("Reviewer_Score")))


    val nationalityCount: RDD[(String, Int)] = coppieNationalityScore.map { case (chiave, _) => (chiave, 1) }.reduceByKey((a, b) => a + b)
     // .filter { case (_, valore) => valore > 5000 }
    val nationalitySumScore: RDD[(String, Double)] = coppieNationalityScore.reduceByKey((a, b) => a + b)

    val nationalityMeanScore = nationalitySumScore.join(nationalityCount).map { case (chiave, (sum, count)) => (chiave, sum / count) }
      .sortBy(coppia => coppia._2, ascending = true)

    nationalityMeanScore.take(30).foreach(println)
    nationalityMeanScore.collectAsMap()
  }

  def getAllNationality: Array[String] = {
    val dati=WebService.dataFrame
    dati.rdd.map(riga => riga.getAs[String]("Reviewer_Nationality")).distinct()
      .sortBy(identity).filter( x=> x!=" ").collect()
  }
}
