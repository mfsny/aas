/*
mkdir audioscrobbler
cd audioscrobbler

curl http://bit.ly/1KiJdOR

wget http://www.iro.umontreal.ca/~lisa/datasets/profiledata_06-May-2005.tar.gz

gunzip profiledata_06-May-2005.tar.gz

ls -la
#-rw------- 1 bik14sn user 485672960 Mar 27  2007 profiledata_06-May-2005.tar

tar -xvf profiledata_06-May-2005.tar

ls -la profiledata_06-May-2005/
#-rw------- 1 bik14sn user   2932731 May  6  2005 artist_alias.txt
#-rw------- 1 bik14sn user  55963575 May  6  2005 artist_data.txt
#-rw------- 1 bik14sn user      1273 May 10  2005 README.txt
#-rw------- 1 bik14sn user 426761761 May  5  2005 user_artist_data.txt

hdfs dfs -mkdir audioscrobbler/
hdfs dfs -put profiledata_06-May-2005/*.txt audioscrobbler/
*/

val rawUserArtistData = sc.textFile("audioscrobbler/user_artist_data.txt")
//rawUserArtistData.first
rawUserArtistData.take(10).foreach(println)
rawUserArtistData.count //24,296,858
rawUserArtistData.map(_.split(' ')(0).toDouble).stats()
rawUserArtistData.map(_.split(' ')(1).toDouble).stats()
rawUserArtistData.map(_.split(' ')(2).toDouble).stats()

val rawArtistData = sc.textFile("audioscrobbler/artist_data.txt")
rawArtistData.take(10).foreach(println)
rawArtistData.count //1,848,707
val artistByID = rawArtistData.map{ line => 
  val (id,name) = line.span(_ != '\t')
  (id.toInt, name.trim)
}

//artistByID.foreach(println)
// !!! causes java.lang.NumberFormatException

val artistByID = rawArtistData.flatMap{ line => 
  val (id,name) = line.span(_ != '\t')
  if (name.isEmpty) {
    None
  } else {
    try {
      Some((id.toInt, name.trim))
    } catch {
      case e: NumberFormatException => None
    }
  }
}

artistByID.take(10).foreach(println)

val rawArtistAlias = sc.textFile("audioscrobbler/artist_alias.txt")
rawArtistAlias.take(10).foreach(println)
rawArtistAlias.count //193,027

val artistAlias = rawArtistAlias.flatMap { line =>
  val tokens = line.split('\t')
  if (tokens(0).isEmpty) {
    None
  } else {
      Some((tokens(0).toInt, tokens(1).toInt))
  }
}.collectAsMap()

artistByID.lookup(6803336).head
artistByID.lookup(1000010).head

import org.apache.spark.mllib.recommendation._

val bArtistAlias = sc.broadcast(artistAlias)

val trainData =  rawUserArtistData.map { line => 
  val Array(userID,artistID,count) = line.split(' ').map(_.toInt)
  val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
  Rating(userID, finalArtistID, count)
}.cache()

bArtistAlias.value.getOrElse(1000010,Double.NaN)
//AnyVal = NaN

bArtistAlias.value.getOrElse(1092764,Double.NaN)
//1000311

val model = ALS.trainImplicit(trainData, 10, 5, .01, 1.0)

model.userFeatures.mapValues(_.mkString(", ")).first()

model.userFeatures.filter(_._1 == 2093760).first

val rawArtistsForUser = rawUserArtistData.map(_.split(' ')).
  filter{case Array(user,_,_) => user.toInt == 2093760 }

rawArtistsForUser.take(10).foreach(x => println(x.mkString(", ")))

val existingProducts = rawArtistsForUser.map { 
  case Array(_,artist,_) => artist.toInt 
}.collect().toSet

artistByID.filter { 
  case (id,name) => existingProducts.contains(id)
}.values.collect().foreach(println)
//David Gray
//Blackalicious
//Jurassic 5
//The Saw Doctors
//Xzibit

val recommendations = model.recommendProducts(2093760, 5)
recommendations.foreach(println)
//Rating(2093760,930,0.008290105584522117)
//Rating(2093760,4267,0.008159590659877033)
//Rating(2093760,1000113,0.008003873558744153)
//Rating(2093760,2814,0.00795340528095233)
//Rating(2093760,1274,0.007895154313770943)

val recommendProductIDs = recommendations.map(_.product).toSet

artistByID.filter { 
  case (id,name) => recommendProductIDs.contains(id)
}.values.collect().foreach(println)
//50 Cent
//The Beatles
//Eminem
//Red Hot Chili Peppers
//Green Day

