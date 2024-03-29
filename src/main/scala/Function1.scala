import org.apache.spark.rdd.RDD

import scala.collection.{Map, MapView}

// 1) Analizzare i commenti negativi in base alla nazionalità richiesta. Dunque capire le preferenze e i confort richiesti per una data nazione.

object Function1 {

  // Esiste un modo piu' elegante per introdurre le parole da escludere?
  private val importantWords = Set("backyard", "room", "clean", "hotel", "aircondition",
    "window", "floor", "dirty", "tv", "fridge", "restaurant", "food", "breakfast",
    "lunch", "dinner", "neighbors", "noisy", "door", "smell", "glass", "shower")
  // rooms, windows, fridges, doors, smelly


  def eseguiAnalisi(): Map[String, List[(String, Double)]] = {

    val colsOfInterest = WebService.dataFrame.select("Reviewer_Nationality", "Negative_Review")


    val rdd_map: RDD[(String, Iterable[Array[String]])]
    = colsOfInterest.rdd
      .map(row => ( row.getString(0), row.getString(1) ))
      .mapValues(value => value
          .split("\\s+")
          .filter(word => importantWords.contains(word)))
      .groupByKey()


    val valuesWordCount: RDD[(String, MapView[String, Int])] = rdd_map.mapValues { iterable =>
      // Eseguo word count sul singolo array
      val wordCount = iterable.flatten
        .filter(word => word.trim.nonEmpty)
        .groupBy(identity) // Raggruppa le parole per valore
        .view.mapValues(_.size) // Calcola la dimensione di ciascun gruppo, ovvero il conteggio delle parole
      // Restituisci il risultato dell'operazione sull'array di stringhe
      wordCount
    }

    val rddOrdinato: RDD[(String, Seq[(String, Int)])] = valuesWordCount.mapValues(_.toSeq.sortBy(-_._2))

    val nationCnt: RDD[(String, Int)] = WebService.dataFrame.select("Reviewer_Nationality").rdd
      .map(row => row.getString(0))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    // Rapporto ogni conteggio di ogni parola per il numero di reviewers della corrispondente nazionalità
    val rapportedRdd: RDD[(String, List[(String, Double)])] = rddOrdinato.join(nationCnt).mapValues(
      element => element._1.map(
        { case (parola, conteggio) => (parola, (conteggio.toDouble / element._2) ) }).toList)
      //.filter { case (_, lista) => lista.nonEmpty }

    // Stampa i risultati
    stampaRisultati(rapportedRdd)

    rapportedRdd.collectAsMap()
  }


  private def stampaRisultati(rddOrdinato: RDD[(String, List[(String,Double)])]): Unit = {
    rddOrdinato.foreach { case (chiave, mappa) =>
      println(s"Chiave: $chiave")
      mappa.foreach { case (parola, conteggio) =>
        println(s"$parola: $conteggio")
      }
    }
  }


}
