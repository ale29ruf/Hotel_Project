import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

// 1) Analizzare i commenti negativi in base alla nazionalità. Dunque capire le preferenze e i confort richiesti per una data nazione.

object Function1 {

  // Esiste un modo piu' elegante per introdurre le parole da escludere?
  private val importantWords = Set("backyard", "room", "rooms", "clean", "hotel", "aircondition", "windows",
    "window", "floor", "dirty", "tv", "fridge", "fridges", "fridges", "restaurant", "food", "breakfast",
    "lunch", "dinner", "neighbors", "noisy", "door", "doors", "smell", "smelly", "glass")

  def eseguiAnalisi(dataFrame: DataFrame): Unit = {
    val colsOfInterest = dataFrame.select("Reviewer_Nationality", "Negative_Review")
    val rdd_map = colsOfInterest.rdd
      .map(row => ( row.getString(0), row.getString(1) ))
      .mapValues(value => value
          .split("\\s+")
          .filter(word => importantWords.contains(word)).mkString(" "))
      .groupByKey().mapValues(iter => iter.mkString(" ").split("\\s+"))

    val valuesWorldCount = rdd_map.mapValues { array =>
      // Qui puoi eseguire le operazioni desiderate sull'array di stringhe
      // Eseguo word count sul singolo array
      val wordCount = array
        .flatMap(_.split("\\s+")) // Split delle stringhe in singole parole
        .groupBy(identity) // Raggruppa le parole per valore
        .view.mapValues(_.length) // Calcola la dimensione di ciascun gruppo, ovvero il conteggio delle parole
      // Restituisci il risultato dell'operazione sull'array di stringhe
      wordCount
    }

    val rddOrdinato = valuesWorldCount.mapValues(_.toSeq.sortBy(-_._2))

    // Stampa i risultati
    stampaRisultati(rddOrdinato)

    println("--------------OUTPUT--------------")
    // Visualizzazione delle prime 5 coppie
    //rdd_coppie_chiave_valore.take(1).foreach(println)
    println("--------------END-OUTPUT--------------")
  }


  // Stampa i risultati
  private def stampaRisultati(rddOrdinato: RDD[(String, Seq[(String, Int)])]): Unit = {
    rddOrdinato.collect().foreach { case (chiave, mappa) =>
      println(s"Chiave: $chiave")
      mappa.foreach { case (parola, conteggio) =>
        println(s"$parola: $conteggio")
      }
    }
  }


}
