import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.Map

// 4) Se il rewiever ha molti Total_Number_of_Reviews_Reviewer_Has_Given allora potrebbe essere uno che scrive
// recensioni tanto per. Dunque la sua recensione potrebbe essere poco affidabile.
// Tramite clustering otteniamo le 3 categorie di significatività dei rewievs considerando length(Rewiev),
// dove Rewiev = PosRewiev + NegRewiev, e Total_Number_of_Reviews_Reviewer_Has_Given. Infine, si calcolano i valori
// percentuali dei rewievs significativi sulla base della nazionalità del rewiev.

object Function2 {

  def eseguiAnalisi(): Map[String, Map[String, Double]] = {

    // Pre-processing dati
    val lenRevWithTotRev = WebService.dataFrame.select("Review_Total_Negative_Word_Counts",
      "Review_Total_Positive_Word_Counts").rdd
      .map(row => {
        val negCnt = row.getString(0).toInt
        val posCnt = row.getString(1).toInt
        negCnt + posCnt })
      .zip(
        WebService.dataFrame.select("Total_Number_of_Reviews_Reviewer_Has_Given").rdd
          .map(_.getString(0).toInt)
      )


    // Occorre convertire revWithTotRev: RDD[(Int, Int)] in un dataFrame per poter usare l'algoritmo di k-means
    // Definiamo prima lo schema che avrà il nuovo dataFrame
    val schema = new StructType()
      .add(StructField("ReviewLength", IntegerType, nullable = true))
      .add(StructField("Total_Number_of_Reviews", IntegerType, nullable = true))

    // Procediamo con la costruzione del nuovo dataFrame usando lo SparkSession.
    // E' necessario prima creare un RDD[Row] a partire da revWithTotRev
    val nuovoDataFrame :DataFrame = Start.spark
      .createDataFrame(lenRevWithTotRev.map{ case (lenRev, totNumRev) => Row(lenRev, totNumRev) }, schema)

    // Assembla le feature in un vettore
    val assembler = new VectorAssembler()
      .setInputCols(Array("ReviewLength", "Total_Number_of_Reviews")) // specifica le colonne da combinare
      .setOutputCol("features") // colonna risultante dei vettori
    val dataset = assembler.transform(nuovoDataFrame)
    // Questo nuovo DataFrame avrà una colonna aggiuntiva chiamata "features", che contiene i vettori combinati delle
    // colonne "ReviewLength" e "Total_Number_of_Reviews". Seguono le prime 3 righe: [408,7,[408.0,7.0]], [105,7,[105.0,7.0]], [63,9,[63.0,9.0]]

    // Addestra il modello KMeans
    val kmeans = new KMeans()
      .setK(3)
      .setSeed(1L)
      .setMaxIter(30) // Imposta il numero massimo di iterazioni
      .setTol(1e-4) // Imposta la soglia di convergenza
      .setPredictionCol("classification") // Specifico il nome della colonna che conterrà le classificazioni
    val model = kmeans.fit(dataset)

    //Mostra i centroidi dei cluster
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    // Ottieni i cluster per ciascun punto
    val classifyDataset = model.transform(dataset)
    // Seguono le prime 3 righe: [408,7,[408.0,7.0],1], [105,7,[105.0,7.0],2], [63,9,[63.0,9.0],2]
    // Lo schema del dataFrame predictions è il seguente:
    // StructType(StructField(ReviewLength,IntegerType,true),StructField(Total_Number_of_Reviews,IntegerType,true),StructField(features,org.apache.spark.ml.linalg.VectorUDT@3bfc3ba7,true),StructField(classification,IntegerType,false))

    val nationalityClass = WebService.dataFrame.select("Reviewer_Nationality").rdd.map(_.getString(0))
      .zip(
        classifyDataset.select("classification").rdd.map(_.getInt(0).toString)
      )


    // Eseguo operazioni sulle coppie (nazionalità, classificazione)
    // Eseguo conteggio delle classi per ogni nazionalità di reviewer
    val result = nationalityClass.groupByKey()
      .mapValues(
        _.flatMap(_.split("\\s+"))
          .groupBy(identity).view.mapValues(_.size).toMap)

    // Segue il conteggio per le prime 4 nazioni:
    //(Jersey, Map(1 -> 21, 0 -> 655, 2 -> 187))
    //(Liberia, Map(0 -> 2, 2 -> 1))
    //(Uzbekistan, Map(0 -> 17, 2 -> 3))
    //(Saint Martin, Map(0 -> 4))

    // Ripondero i conteggi
    val wordCountsNationality = WebService.dataFrame.select("Reviewer_Nationality").rdd
      .map(row => row.getString(0))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .persist(StorageLevel.MEMORY_ONLY)

    val repoundResult = result.join(wordCountsNationality)
      .map { case (key, (map, totRevNat)) =>
      val repoundedMap = map.view.mapValues( count => (count.toDouble / totRevNat) * 100 ).toMap // divido ogni valore intero della mappa per il numero totale di viewer per quella stessa nazionalità
      (key, repoundedMap) }
      //.filter { case (nation, _) => nation == " "+nationality+" " }

    // Segue la stampa delle prime 3 nazionalità:
    // (Jersey, Map(1 -> 2.4333719582850524, 0 -> 75.89803012746235, 2 -> 21.668597914252608))
    // (Liberia, Map(0 -> 66.66666666666666, 2 -> 33.33333333333333))
    // (Uzbekistan, Map(0 -> 85.0, 2 -> 15.0))

    // Estrarre la chiave con il valore più alto '1' (classe più significativa
    val keyWithMaxValueOne = repoundResult.mapValues(_.getOrElse("1", 0.0)) // Ottenere il valore '1' dalla mappa, se non presente usare 0.0
      .reduce((x, y) => if (x._2 > y._2) x else y) // Ridurre al massimo valore '1'

    // Stampa della chiave con il valore '1' più alto
    println("Chiave con il valore '1' più alto: " + keyWithMaxValueOne._1)

    repoundResult.collectAsMap()
  }

}
