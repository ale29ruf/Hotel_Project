import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

// 1) Analizzare i commenti negativi in base alla nazionalità. Dunque capire le preferenze e i confort richiesti per una data nazione.

object Function1 {

  // Esiste un modo piu' elegante per introdurre le parole da escludere?
  private val importantWords = Set("backyard", "room", "rooms", "clean", "hotel", "aircondition", "windows",
    "window", "floor", "dirty", "tv", "fridge", "fridges", "restaurant", "food", "breakfast",
    "lunch", "dinner", "neighbors", "noisy", "door", "doors", "smell", "smelly", "glass", "shower")

  def eseguiAnalisi(dataFrame: DataFrame): Unit = {
    val colsOfInterest = dataFrame.select("Reviewer_Nationality", "Negative_Review")

    val nationCnt = dataFrame.select("Reviewer_Nationality").rdd
      .map(word => (word.getString(0), 1))
      .reduceByKey(_ + _)


    val rdd_map = colsOfInterest.rdd
      .map(row => ( row.getString(0), row.getString(1) ))
      .mapValues(value => value
          .split("\\s+")
          .filter(word => importantWords.contains(word)).mkString(" "))
      .groupByKey()


    val valuesWordCount = rdd_map.mapValues { array =>
      // Qui puoi eseguire le operazioni desiderate sull'array di stringhe
      // Eseguo word count sul singolo array
      val wordCount = array
        .flatMap(_.split("\\s+")) // Split delle stringhe in singole parole
        .filter(word => word.trim.nonEmpty)
        .groupBy(identity) // Raggruppa le parole per valore
        .view.mapValues(_.size) // Calcola la dimensione di ciascun gruppo, ovvero il conteggio delle parole
      // Restituisci il risultato dell'operazione sull'array di stringhe
      wordCount
    }

    val rddOrdinato = valuesWordCount.mapValues(_.toSeq.sortBy(-_._2))

    // Rapporto ogni conteggio di ogni parola per il numero di reviewers della corrispondente nazionalità
    val rapportedRdd = rddOrdinato.join(nationCnt).mapValues(
      element => element._1.map(
        { case (parola, conteggio) => (parola, (conteggio.toDouble / element._2)*100 ) }))


    // Stampa i risultati
    stampaRisultati(rapportedRdd)

    println("--------------END-OUTPUT--------------")
  }


  private def stampaRisultati(rddOrdinato: RDD[(String, Seq[(String,Double)])]): Unit = {
    rddOrdinato.foreach { case (chiave, mappa) =>
      println(s"Chiave: $chiave")
      mappa.foreach { case (parola, conteggio) =>
        println(s"$parola: $conteggio")
      }
    }
  }


}
