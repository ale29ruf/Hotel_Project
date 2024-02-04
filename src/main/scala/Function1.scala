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
      .mapValues(value => {
        value.split("\\s+")
        .foldLeft(Set.empty[String])((set, word) => set + word)
        .intersect(importantWords).mkString(" ")})
      //.map(kv => kv._1 + "\t" + kv._2)
      //.saveAsTextFile("output")

    println("--------------OUTPUT--------------")
    // Visualizzazione delle prime 5 coppie
    //rdd_coppie_chiave_valore.take(10).foreach(println)
    println("--------------END-OUTPUT--------------")
  }

}
