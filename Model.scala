package model

import java.sql.Timestamp

case class Rating(user: Long, movie: Long, rating: Int, timestamp: Timestamp)
case class Product(prod_id: Long, prod_name: String)

// Music dataset
case class Artist(aid: Long, aname: String)
case class UserArtist(uid: Long, aid: Long, count: Int)
case class Alias(badaid: Long, goodaid: Long )

// MovieLens dataset
case class Movie(movid: Long, movtitle: String, genre: String)
case class MovieRating(uid: Long, movid: Long, rating: Double, movtime: Timestamp)