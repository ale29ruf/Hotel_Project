import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}
import scala.io.Source


class ClusteringHotelScoreCITTA {
}

/*
object ClusteringHotelScoreCITTA{

  def citta(latitude: Double, longitude: Double){
    val apiKey =
    val url = s"https://maps.googleapis.com/maps/api/geocode/json?latlng=$latitude,$longitude&key=$apiKey"

    // Esegui la richiesta HTTP e ottieni la risposta come stringa JSON
    val jsonResponse = Source.fromURL(url).mkString

    // Analizza la risposta JSON per ottenere le informazioni sulla città
    val city = parseCityFromJson(jsonResponse)
    city
  }

  def parseCityFromJson(json: String): String = {
    // Qui dovresti implementare la logica per analizzare il JSON e ottenere il nome della città
    // Ad esempio, potresti usare una libreria JSON come circe per effettuare il parsing
    // Ma per brevità, in questo esempio assumiamo che la città sia estratta direttamente dal JSON
    "City Name" // Cambia questa linea con la logica di parsing reale
  }
}

  def main(args: Array[String]){
    val inputFile = "C:\\Users\\asus\\Desktop\\progetto_big_data\\Hotel_Reviews.csv"
    val spark = SparkSession.builder.master("local[*]").appName("HotelReviewsAnalysis").getOrCreate()



    val dati: DataFrame = spark.read
      .option("header", "true") // Se la prima riga è l'intestazione
      .option("inferSchema", "true") // Inferisci automaticamente il tipo di dati delle colonne
      .csv(inputFile)

    val dati_distinti_per_hotel: DataFrame= dati.dropDuplicates("Hotel_Address")
    val coordinateEncoder: Encoder[Coordinate] = Encoders.product[Coordinate]






    val assembler = new VectorAssembler()
      .setInputCols(Array("Average_Score"))
      .setOutputCol("features")

    val assembledScores = assembler.transform(dati_distinti_per_hotel)


    val kmeans = new KMeans().setK(3).setSeed(1L).setPredictionCol("prediction")
    val model = kmeans.fit(assembledScores)

    val dataFrameClassified: DataFrame = model.transform(assembledScores)


    // Fornisci un implicit Encoder per il tuo tipo di dato
    implicit val encoder: Encoder[(String, Int)] = Encoders.product[(String, Int)]

    val RDD_Citta_LEVEL : RDD[(String, Int)]= dataFrameClassified.select("Hotel_Address", "prediction").
      map(row => (citta(row.getAs[String]("Hotel_Address")), row.getAs[Int]("prediction")))(encoder).rdd

    val RDD_Citta_CountHotel: RDD[(String, Int)]= RDD_Citta_LEVEL.map { case (chiave, _) => (chiave, 1)}.reduceByKey((a, b)=> a+b)

    val RDD_Citta_GOOD: RDD[(String, Int)]= RDD_Citta_LEVEL
      .filter( coppia=> coppia._2==2)
      .map {case(chiave, _) =>(chiave, 1)}
      .reduceByKey(_+_)

    val RDD_Citta_INTERMEDIATE: RDD[(String, Int)] = RDD_Citta_LEVEL
      .filter(coppia => coppia._2 == 1)
      .map { case (chiave, _) => (chiave, 1) }
      .reduceByKey(_ + _)

    val RDD_Citta_BAD: RDD[(String, Int)] = RDD_Citta_LEVEL
      .filter(coppia => coppia._2 == 0)
      .map { case (chiave, _) => (chiave, 1) }
      .reduceByKey(_ + _)

    val RDD_Citta_Percent_GOOD = RDD_Citta_GOOD.join(RDD_Citta_CountHotel).map{case (chiave, (sum, count)) => (chiave, sum.toDouble /count*100)}
    val RDD_Citta_Percent_INTERMEDIATE = RDD_Citta_INTERMEDIATE.join(RDD_Citta_CountHotel).map{case (chiave, (sum, count)) => (chiave, sum.toDouble /count*100)}
    val RDD_Citta_Percent_BAD = RDD_Citta_BAD.join(RDD_Citta_CountHotel).map{case (chiave, (sum, count)) => (chiave, sum.toDouble /count*100)}



    RDD_Citta_Percent_GOOD.foreach(println)
    println("---Percentuale hotel buoni per citta---")


    RDD_Citta_Percent_INTERMEDIATE.foreach(println)
    println("---Percentuale hotel intermedi per citta---")


    RDD_Citta_Percent_BAD.foreach(println)
    println("---Percentuale hotel scarsi per citta---")

    //Mostra i centroidi dei cluster
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
  }
}
*/