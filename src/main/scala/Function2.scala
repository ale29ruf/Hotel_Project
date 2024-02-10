import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame

// 4) Se il rewiever ha molti Total_Number_of_Reviews_Reviewer_Has_Given allora potrebbe essere uno che scrive
// recensioni tanto per. Dunque la sua recensione potrebbe essere poco affidabile.
// Tramite clustering otteniamo le 3 categorie di significatività dei rewievs considerando length(Rewiev),
// dove Rewiev = PosRewiev + NegRewiev, e Total_Number_of_Reviews_Reviewer_Has_Given. Infine, si calcolano i valori
// percentuali dei rewievs significativi sulla base della nazionalità del rewiev.

object Function2 {

  def eseguiAnalisi(dataFrame: DataFrame): Unit = {

    // Pre-processing dati
    val revWithTotRev = dataFrame.select("Negative_Review", "Positive_Review").rdd
      .map(row => {
        val negRev = row.getString(0)
        val posRev = row.getString(1)
        val concatRev = s"$negRev $posRev"
        concatRev })
      .zip(dataFrame.select("Total_Number_of_Reviews").rdd.map(_.getInt(0)))

    val revWithTotRev2 = dataFrame.select("Negative_Review", "Positive_Review").rdd
      .map(row => {
        val negRev = row.getString(0)
        val posRev = row.getString(1)
        val concatRev = s"$negRev $posRev"
        concatRev
      })
      .zip(dataFrame.select("Total_Number_of_Reviews").rdd.map(_.getInt(0)))

    // Assembla le feature in un vettore
    val assembler = new VectorAssembler().setInputCols(Array("feature1", "feature2")).setOutputCol("features")
    val dataset = assembler.transform(dataFrame)

    // Crea il modello K-Means
    val kmeans = new KMeans().setK(3).setSeed(1L)
    val model = kmeans.fit(dataset)

    // Esegui il clustering
    val predictions = model.transform(dataset)


  }

}
