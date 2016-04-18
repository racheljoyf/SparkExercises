package exercises

import java.nio.file.Paths
import java.sql.Timestamp

import model.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Rachel Florentino on 18/04/2016.
  */
object Exercise_1 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Workshop").setMaster("local[*]")
    val url: String = Paths.get(getClass.getResource("/ratings.txt").toURI).toAbsolutePath.toString
    val sc: SparkContext = new SparkContext(conf)

    val lines: RDD[Rating] = sc.textFile(url).map(_.split('\t')).map(row => Rating(row(0).toLong, row(1).toLong, row(2).toInt, new Timestamp(row(3).toLong * 1000)))

    /**********************************************************************
      * #1: Determine how many movies are rated below AND above the average
      **********************************************************************/
    val ratingsAve: Int = lines.map(_.rating).mean().toInt
    val cachedRDD: RDD[(Long, Int)] = lines.map(rating => (rating.movie, rating.rating)).persist()

    val rddMoviesAboveAve: RDD[(Long, Boolean)] = cachedRDD.map(x => (x._1, if (x._2 > ratingsAve) true else false))
    val rddMoviesBelowAve: RDD[(Long, Boolean)] = cachedRDD.map(x => (x._1, if (x._2 < ratingsAve) true else false))
    cachedRDD.unpersist(false)

    val rddMoviesAboveAveGrp: RDD[(Long, Boolean)] = rddMoviesAboveAve.filter(row => row._2 == true)
    val rddMoviesBelowAveGrp: RDD[(Long, Boolean)] = rddMoviesBelowAve.filter(row => row._2 == true)
    val countMoviesAboveAndBelowAve: Long = rddMoviesAboveAveGrp.keys.distinct.intersection(rddMoviesBelowAveGrp.keys.distinct).count()

    // Results for #1: 1,353
    println( s"""Number of movies rated below AND above the average = $countMoviesAboveAndBelowAve""")

    /**********************************************************************
      * #2: Determine how many users gave more votes above the mean than below it.
      **********************************************************************/
    val cachedRDD2: RDD[(Long, Int)] = lines.map(rating => (rating.user, rating.rating))
    val rddUserVsVotes: RDD[(Long, Int)] = cachedRDD2.map(row => (row._1,
      if (row._2 > ratingsAve) 1
      else if (row._2 < ratingsAve) -1
      else 0)
    ).reduceByKey(_ + _)

    val rddUserWithVotesAboveAve = rddUserVsVotes.filter(row => if(row._2 > 0) true else false)
    val countUserWithVotesAboveAve = rddUserWithVotesAboveAve.count()

    println( s"""Number of users who gave more votes above the mean than below it = $countUserWithVotesAboveAve""") // 859
  }
}
