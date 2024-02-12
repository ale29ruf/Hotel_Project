import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import scala.collection.Map

// 1) Analizzare i commenti negativi in base alla nazionalità richiesta. Dunque capire le preferenze e i confort richiesti per una data nazione.

object Function1 {

  // Esiste un modo piu' elegante per introdurre le parole da escludere?
  private val importantWords = Set("backyard", "room", "clean", "hotel", "aircondition",
    "window", "floor", "dirty", "tv", "fridge", "restaurant", "food", "breakfast",
    "lunch", "dinner", "neighbors", "noisy", "door", "smell", "glass", "shower")
  // rooms, windows, fridges, doors, smelly


  def eseguiAnalisi(nationality: String): Map[String, List[(String, Double)]] = {

    val colsOfInterest = WebService.dataFrame.select("Reviewer_Nationality", "Negative_Review")

    val nationalityMod = " "+nationality+" "

    val nationCnt = WebService.dataFrame.select("Reviewer_Nationality").rdd
      .map(row => row.getString(0))
      .filter(_ == nationalityMod) // filtraggio in base alla nazionalità passata
      .map(word => (word, 1))
      .reduceByKey(_ + _)


    println("Conteggio nazione" + nationCnt.count)

    val rdd_map = colsOfInterest.rdd
      .map(row => ( row.getString(0), row.getString(1) ))
      .filter { case (firstString, _) => firstString == nationalityMod } // filtraggio in base alla nazionalità passata
      .mapValues(value => value
          .split("\\s+")
          .filter(word => importantWords.contains(word)).mkString(" "))
      .groupByKey()


    val valuesWordCount = rdd_map.mapValues { array =>
      // Qui puoi eseguire le operazioni desiderate sull'array di stringhe
      // Eseguo word count sul singolo array
      val wordCount = array
        .flatMap(_.split("\\s+")) // la flatMap consente di appiattire gli array di stringhe che genera "split"
        // e di avere un RDD[String] piuttosto che un RDD[String[]]
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
        { case (parola, conteggio) => (parola, (conteggio.toDouble / element._2)*100 ) }).toList)
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
