import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

// 4) Se il rewiever ha molti Total_Number_of_Reviews_Reviewer_Has_Given allora potrebbe essere uno che scrive
// recensioni tanto per. Dunque la sua recensione potrebbe essere poco affidabile.
// Tramite clustering otteniamo le 3 categorie di significatività dei rewievs considerando length(Rewiev),
// dove Rewiev = PosRewiev + NegRewiev, e Total_Number_of_Reviews_Reviewer_Has_Given. Infine, si calcolano i valori
// percentuali dei rewievs significativi sulla base della nazionalità del rewiev.

object Function2 {

  def eseguiAnalisi(dataFrame: DataFrame): Unit = {

    // Pre-processing dati
    val lenRevWithTotRev = dataFrame.select("Review_Total_Negative_Word_Counts", "Review_Total_Positive_Word_Counts").rdd
      .map(row => {
        val negCnt = row.getString(0).toInt
        val posCnt = row.getString(1).toInt
        negCnt + posCnt })
      .zip(dataFrame.select("Total_Number_of_Reviews_Reviewer_Has_Given").rdd.map(_.getString(0).toInt))

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
    // colonne "feature1" e "feature2". Seguono le prime 3 righe: [408,7,[408.0,7.0]], [105,7,[105.0,7.0]], [63,9,[63.0,9.0]]

    // Addestra il modello KMeans
    val kmeans = new KMeans()
      .setK(3)
      .setSeed(1L)
      .setMaxIter(30) // Imposta il numero massimo di iterazioni
      .setTol(1e-4) // Imposta la soglia di convergenza
    val model = kmeans.fit(dataset)

    //Mostra i centroidi dei cluster
    //println("Cluster Centers: ")
    //model.clusterCenters.foreach(println)

    // Ottieni i cluster per ciascun punto
    val predictions = model.transform(dataset)
    // Seguono le prime 3 righe: [408,7,[408.0,7.0],1], [105,7,[105.0,7.0],2], [63,9,[63.0,9.0],2]
    // Lo schema del dataFrame predictions è il seguente:
    // StructType(StructField(ReviewLength,IntegerType,true),StructField(Total_Number_of_Reviews,IntegerType,true),StructField(features,org.apache.spark.ml.linalg.VectorUDT@3bfc3ba7,true),StructField(prediction,IntegerType,false))



  }

}
