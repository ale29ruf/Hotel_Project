import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.annotation.unused

object Function6{

  def getAllPairs(strings: Array[String]):Array[(String, String)]  =
  {
    for (i1<- strings; i2<- strings; if(i1.compareTo(i2)<0))
      //Prendo tutte le coppie ma
      // evito di prendere coppie simmetriche due volte,
      //evito di prendere coppie di elemenri uguali
      yield(i1, i2) //Permette di costruire una collezione di coppie
  }

  def cleanStringa(stringa: String): String=
  { // Definisci l'espressione regolare per identificare parentesi quadre, virgole
      val regex = "[^a-zA-Z0-9]"
      val cleanedStringa = stringa.replaceAll(regex, " ")
      cleanedStringa
  }
  def eseguiAnalisi: List[List[String]] ={

    val dati=WebService.dataFrame

    val items: RDD[Array[String]]= dati.map (riga => riga.getAs[String]("Tags"))
      .map(item => item.split(","))  //Suddivido per virgole
      .map( array=> array.map(stringa =>cleanStringa(stringa).trim)) //Ripulisco ogni stringa di ogni array

    val coppie: RDD[(String,String)] = items
      .filter( item => item.length>=2)
      .flatMap(item => getAllPairs(item))

    val coppieOcc: RDD[(List[String], Int)]= coppie
      .map(coppia =>(List(coppia._1, coppia._2), 1))
      .reduceByKey((a, b)=> a+b)

    val ordCoppieOcc: RDD[List[String]] = coppieOcc
      .sortBy( coppia=> coppia._2, ascending=false)
      .map{ case (a,_)=>a}
    ordCoppieOcc.collect().toList.take(45)
  }
}
