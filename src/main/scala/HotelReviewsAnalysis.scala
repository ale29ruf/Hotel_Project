import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util

class HotelReviewsAnalysis {
  }

object HotelReviewAnalysis {
  def main(args: Array[String]) {
    val inputFile = "C:\\Users\\asus\\Desktop\\progetto_big_data\\Hotel_Reviews.csv"

    /*
      - SparkSession.builder: Inizia la costruzione di un oggetto SparkSession
      - master("local[*]"): Imposta la modalità di esecuzione di Spark.
      Spark utilizzerà tutti i core disponibili sulla macchina locale per l'esecuzione.
      - appName("HotelReviewsAnalysis"): Assegna un nome all 'applicazione Spark in esecuzione
      - getOrCreate(): Restituisce o crea un oggetto SparkSession
     */
    val spark = SparkSession.builder.master("local[*]").appName("HotelReviewsAnalysis").getOrCreate()

    val dati: DataFrame = spark.read
      .option("header", "true") // Se la prima riga è l'intestazione
      .option("inferSchema", "true") // Inferisci automaticamente il tipo di dati delle colonne
      .csv(inputFile)

    val RDDReviews: RDD[String] = dati.select("Positive_Review").rdd.map(r => r.getString(0))
    val wordCount= RDDReviews.flatMap(s => s.split (" ")).map(s => (s, 1)).reduceByKey((a, b)=> a+b).
      sortBy { case (_, valore) => valore }

    wordCount.saveAsTextFile("C:\\Users\\asus\\Desktop\\file.txt")
  }
}
