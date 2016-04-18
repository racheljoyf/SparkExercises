package exercises

import java.nio.file.Paths
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{GregorianCalendar, Calendar}

import model.MovieRating
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Rachel Florentino on 15/04/2016.
  */

object Exercise_MovieLens{

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Workshop").setMaster("local[*]")
    val url_rating: String = Paths.get(getClass.getResource("/ratings.csv").toURI).toAbsolutePath.toString
    val url_ex1: String = ("C:\\_Rachel\\DSTI\\Courses\\0005_C1002_SPARK\\spark-exercices\\src\\main\\resources\\MovieLens_Ex1")
    val url_ex2: String = ("C:\\_Rachel\\DSTI\\Courses\\0005_C1002_SPARK\\spark-exercices\\src\\main\\resources\\MovieLens_Ex2")
    val url_ex3: String = ("C:\\_Rachel\\DSTI\\Courses\\0005_C1002_SPARK\\spark-exercices\\src\\main\\resources\\MovieLens_Ex3")
    val url_ex4: String = ("C:\\_Rachel\\DSTI\\Courses\\0005_C1002_SPARK\\spark-exercices\\src\\main\\resources\\MovieLens_Ex4")
    val sc: SparkContext = new SparkContext(conf)

    // Read ratings dataset
    val baselinesRatings: RDD[Seq[String]] = sc.textFile(url_rating).map(_.split(','))
    // Remove file header
    val baselines_noheader: RDD[Seq[String]] = baselinesRatings.filter(row => (!row.contains("userId")))

    // Transform to RDD
    val linesRatings: RDD[MovieRating] = baselines_noheader.map(row => MovieRating(row(0).toLong, row(1).toLong, row(2).toDouble, new Timestamp(row(3).toLong * 1000)))

    /**********************************************************************
      * #1: How many new users every month
      ***********************************************************************/
    // Create new RDD of user VS rating time.
    val rddUserRatingtime: RDD[(Long, Timestamp)] = linesRatings.map(row => (row.uid, row.movtime))

    // Search for the earliest date when the user submitted a rating (e.g. user's joining date)
    val createDateCombiner: (Timestamp) => Timestamp = (earliestDate: Timestamp) => (earliestDate)
    val dateCombiner: (Timestamp, Timestamp) => Timestamp = (acc: Timestamp, earliestDate: Timestamp) => {
      (if (acc.compareTo(earliestDate) > 0) earliestDate else acc)
    }
    val dateMerger: (Timestamp, Timestamp) => Timestamp = (acc1: Timestamp, acc2: Timestamp) => {
      (if (acc1.compareTo(acc2) > 0) acc1 else acc2)
    }
    val rddUserJoindate: RDD[(Long, Timestamp)] = rddUserRatingtime.combineByKey(createDateCombiner, dateCombiner, dateMerger)

    // Reformat the join dates to MM-YYYY.
    val rddUserSimplejoindate = rddUserJoindate.map{row => (row._1,
      {
        val df: SimpleDateFormat = new SimpleDateFormat("MM/yyyy")
        df.format(row._2)
      }
    )}

    // Create new RDD of the new formatted join date VS user id.
    val rddJoindateUser: RDD[(String, Long)] = rddUserSimplejoindate.map(row => (row._2, row._1))
    // Count all users for each join date.
    val createSimpledateCombiner: (Long) => Int = (uid: Long) => (1)
    val simpledateCombiner: (Int, Long) => Int = (acc: Int, score: Long) => {
      (acc + 1)
    }
    val simpledateMerger: (Int, Int) => Int = (acc1: Int, acc2: Int) => {
      (acc1 + acc2)
    }

    // Results for #1 => (MM/YYYY, # of new users)
    val rddNewUsersPerMonth: RDD[(String, Int)] = rddJoindateUser.combineByKey(createSimpledateCombiner, simpledateCombiner, simpledateMerger).sortByKey()
    // Save to file
    rddNewUsersPerMonth.saveAsTextFile(url_ex1)


    /**********************************************************************
      * #2: How many users are leaving (assuming inactive for 3 months)
      ***********************************************************************/
    // Search for the latest date when the user submitted a rating
    val createLastratingdateCombiner: (Timestamp) => Timestamp = (lastratingDate: Timestamp) => (lastratingDate)
    val lastratingdateCombiner: (Timestamp, Timestamp) => Timestamp = (acc: Timestamp, lastratingDate: Timestamp) => {
      (if (acc.compareTo(lastratingDate) < 0) lastratingDate else acc)
    }
    val lastratingdateMerger: (Timestamp, Timestamp) => Timestamp = (acc1: Timestamp, acc2: Timestamp) => {
      (if (acc1.compareTo(acc2) < 0) acc1 else acc2)
    }
    val rddUserLastratingdate: RDD[(Long, Timestamp)] = rddUserRatingtime.combineByKey(createLastratingdateCombiner, lastratingdateCombiner, lastratingdateMerger)

    // Reformat to ==> (userid, month of last rating, year of last rating)
    val formatMonth: SimpleDateFormat = new SimpleDateFormat("MM")
    val formatYear: SimpleDateFormat = new SimpleDateFormat("yyyy")
    val rddUserLastratingMonthDay: RDD[(Long, String, String)] = rddUserLastratingdate.map(row => (row._1, formatMonth.format(row._2), formatYear.format(row._2)))

    // From README.txt: Last possible date is March 31, 2015.
    val format = new SimpleDateFormat("MM/yyyy")
    val theDate = format.parse("03/2015")

    val myCal = new GregorianCalendar()
    myCal.setTime(theDate)

    val lastPossibleRatingYear: Int = myCal.get(Calendar.YEAR)
    val lastPossibleRatingMonth: Int = myCal.get(Calendar.MONTH) + 1

    val rddUserInactiveMonths = rddUserLastratingMonthDay.map{ row => (row._1,
      {
        val diffInYears = lastPossibleRatingYear - (row._3.toInt)
        val diffInMonths = lastPossibleRatingMonth - (row._2.toInt)
        if(diffInMonths < 0)
          (diffInYears * 12 + (lastPossibleRatingMonth + (row._2.toInt)) % 12)
        else
          (diffInYears * 12 + diffInMonths)
      })
    }

    // Results for #2 => => (user ID, # of months that the user has been inactive)
    // Filter users who have been inactive for more than 3 months
    val rddInactiveUsers: RDD[(Long, Int)] = rddUserInactiveMonths.filter(row => (row._2 > 3))
    // Save to file
    rddInactiveUsers.saveAsTextFile(url_ex2)

    // For printing purposes
    val numInactiveUsers = rddInactiveUsers.count()
    println( s"""Number of inactive users = $numInactiveUsers""")


    /**********************************************************************
      * #3: How many new ratings every month
      ***********************************************************************/
    // Create new RDD of rating VS rating time.
    val rddRatingMovietime: RDD[(Double, Timestamp)] = linesRatings.map(row => (row.rating, row.movtime))

    // Reformat the rating dates to MM-YYYY.
    val rddRatingSimpletime: RDD[(Double, String)] = rddRatingMovietime.map{ row => (row._1,
      {
        val df: SimpleDateFormat = new SimpleDateFormat("MM/yyyy")
        df.format(row._2)
      }
      )}

    // Create new RDD of the new formatted join date VS rating.
    val rddRatingdateRating: RDD[(String, Double)] = rddRatingSimpletime.map(row => (row._2, row._1))
    // Count all ratings for each rating month.
    val createRatingdateCombiner: (Double) => Int = (rating: Double) => (1)
    val ratingdateCombiner: (Int, Double) => Int = (acc: Int, rating: Double) => {
      (acc + 1)
    }
    val ratingdateMerger: (Int, Int) => Int = (acc1: Int, acc2: Int) => {
      (acc1 + acc2)
    }

    // Results for #3 => (MM/YYYY, # of ratings)
    val rddRatingsPerMonth: RDD[(String, Int)] = rddRatingdateRating.combineByKey(createRatingdateCombiner, ratingdateCombiner, ratingdateMerger)
    // Save to file
    rddRatingsPerMonth.saveAsTextFile(url_ex3)

    /**********************************************************************
      * 4.	Mean + Standard Deviation of 10% most active users based on the average of their ratings
      ***********************************************************************/
    // Create new RDD of user VS rating.
    val rddUserRating = linesRatings.map(row => (row.uid, row.rating))

    // Find out how many times a user gave a rating.
    val createRatingcountCombiner: (Double) => Int = (rating: Double) => (1)
    val ratingcountCombiner: (Int, Double) => Int = (acc: Int, rating: Double) => {
      (acc + 1)
    }
    val ratingCountMerger: (Int, Int) => Int = (acc1: Int, acc2: Int) => {
      (acc1 + acc2)
    }
    val rddUserRatingcount: RDD[(Long, Int)] = rddUserRating.combineByKey(createRatingcountCombiner, ratingcountCombiner, ratingCountMerger).sortBy(-_._2)

    // Create new RDD of rating count VS user ID.
    val rddRatingcountUser: RDD[(Int, Long)] = rddUserRatingcount.map(row =>(row._2, row._1))

    // Find out: (number of ratings/times VS number of users who voted this many ratings/times)
    val createRatingcountCombiner2: (Long) => Int = (uid: Long) => (1)
    val ratingcountCombiner2: (Int, Long) => Int = (acc: Int, uid: Long) => {
      (acc + 1)
    }
    val ratingCountMerger2: (Int, Int) => Int = (acc1: Int, acc2: Int) => {
      (acc1 + acc2)
    }
    val rddRatingcount: RDD[(Int, Int)] = rddRatingcountUser.combineByKey(createRatingcountCombiner2, ratingcountCombiner2, ratingCountMerger2).sortBy(-_._2)

    // Get the top 10% rating counts/times.
    val top10percent: Double = rddRatingcount.count() * 0.10

    val rddRatingcountUserTop: RDD[(Int, Long)] = rddRatingcountUser.subtract {   // 3. Finally, get ratings which ARE in the top 10%
      rddRatingcountUser.subtractByKey {                                          // 2. Get ratings which are NOT in the top 10%
        sc.parallelize(rddRatingcount.top(top10percent.toInt))                    // 1. Find top 10% of number of ratings.
      }
    }

    // Create RDD for printing results => (user ID, # of rating times/counts)
    val rddPrintActiveUsers = rddRatingcountUserTop.map(row => (row._2, row._1))

    // Get the TOP 10% from the original RDD (user ID VS actual ratings)
    val rddTopActiveUsers: RDD[(Long, Double)] = rddUserRating.subtract{      // 2. Finally, get uid's VS ratings which ARE in the top 10%
      rddUserRating.subtractByKey(rddPrintActiveUsers)                        // 1. Get uid's VS ratings which are NOT in top 10%.
    }

    // Create RDD to contain just the ratings of top 10% active users.
    val rddTopRatings: RDD[Double] = rddTopActiveUsers.map(row => (row._2))

    // Compute mean and standard deviation.
    val count: Long = rddTopRatings.count
    val mean: Double = rddTopRatings.reduce(_ + _) / count
    val devs: RDD[Double] = rddTopRatings.map(rating => (rating - mean) * (rating - mean))
    val stddev: Double = Math.sqrt(devs.sum / count)

    // Results for #4
    println(s"""Mean = $mean""" )
    println(s"""Standard Deviation = $stddev""" )

    // Just for printing top 10% active users to file.
    // Save to file
    rddPrintActiveUsers.saveAsTextFile(url_ex4)
  }
}