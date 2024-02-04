import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
// 1) Analizzare i commenti negativi in base alla nazionalità. Dunque capire le preferenze e i confort richiesti per una data nazione.

object Function1 {

  private type MapResult = Map[String, List[String]]

  // Esiste un modo piu' elegante per introdurre le parole da escludere?
  private val importantWords = Set("backyard", "room", "rooms", "clean", "hotel", "aircondition", "windows",
    "window", "floor", "dirty", "tv", "fridge", "fridges", "fridges", "restaurant", "food", "breakfast",
    "lunch", "dinner", "neighbors", "noisy", "door", "doors", "smell", "smelly", "glass")

  def eseguiAnalisi(dataFrame: DataFrame): Unit = {
    val colsOfInterest = dataFrame.select("Reviewer_Nationality", "Negative_Review")
    val rdd_coppie_chiave_valore = colsOfInterest.rdd
      .map(row => ( row.getString(0), row.getString(1) ))
      .mapValues(value =>
        value
          .split("\\s+")
          .filter(word => importantWords.contains(word)).mkString(" "))

    // Ottenere un array di RDD[String] con il parsing dei valori delle chiavi
    val arrayRDD: Array[RDD[String]] = rdd_coppie_chiave_valore.map { case (chiave, valore) =>
      // Esegui il parsing dei valori delle chiavi
      Start.spark.sparkContext.parallelize(valore.split("\\s+")) }.collect()
    // arrayRDD contiene un array di RDD[String], ciascuno contenente il parsing dei valori delle chiavi



    println("--------------OUTPUT--------------")
    // Visualizzazione delle prime 5 coppie
    //rdd_coppie_chiave_valore.take(1).foreach(println)
    println("--------------END-OUTPUT--------------")
  }

}
