import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

class NationalityScoreAnalysis {
}

object NationalityScoreAnalysis{
  def main(args: Array[String]) {
    val inputFile = "C:\\Users\\asus\\Desktop\\progetto_big_data\\Hotel_Reviews.csv"
    val spark = SparkSession.builder.master("local[*]").appName("HotelReviewsAnalysis").getOrCreate()


    val dati: DataFrame = spark.read
      .option("header", "true") // Se la prima riga è l'intestazione
      .option("inferSchema", "true") // Inferisci automaticamente il tipo di dati delle colonne
      .csv(inputFile)

    val coppieNationalityScore: RDD[(String, Double)] =
      dati.rdd.map(riga => (riga.getAs[String]("Reviewer_Nationality"),
        riga.getAs[Double]("Reviewer_Score")))


    val nationalityCount: RDD[(String, Int)] = coppieNationalityScore.map { case (chiave, _) => (chiave, 1) }.reduceByKey((a, b) => a + b)
      .filter{ case (_, valore)=> valore>5000}
    val nationalitySumScore: RDD[(String, Double)] = coppieNationalityScore.reduceByKey((a, b) => a + b)

    val nationalityMeanScore = nationalitySumScore.join(nationalityCount).map { case (chiave, (sum, count)) => (chiave, sum / count) }
      .sortBy(coppia => coppia._2, ascending = true)

    nationalityMeanScore.take(30).foreach(println)
  }
}
