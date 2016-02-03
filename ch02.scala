//Advanced Analytics with Spark
/*
mkdir linkage
cd linkage

curl -L -o donation.zip http://bit.ly/1Aoywaq

unzip  donation.zip
unzip 'block_*.zip'

hdfs dfs -mkdir linkage/
hdfs dfs -put block_*.csv linkage/
hdfs dfs -ls linkage/

$ spark-shell
*/
val rawblocks = sc.textFile("linkage")

rawblocks.first

val head = rawblocks.take(10)

head.foreach(println)

def isHeader(line: String) = line.contains("id_1")

head.filter(isHeader).foreach(println)

head.filterNot(isHeader).length

head.filter(x => !isHeader(x)).length

head.filter(!isHeader(_)).length

val noheader = rawblocks.filter(!isHeader(_))

val line = head(5)
val pieces = line.split(',')

val id1 = pieces(0).toInt
val id2 = pieces(1).toInt
val matched = pieces(11).toBoolean

val rawscores = pieces.slice(2,11)
//rawscores.map(s => s.toDouble) ERROR
def toDouble (s : String ) = {
  if ( "?".equals(s) ) Double.NaN else s.toDouble
}
rawscores.map(toDouble)

def parse ( line : String ) = {
  val pieces = line.split(',')
  val id1 = pieces(0).toInt
  val id2 = pieces(1).toInt
  val scores = pieces.slice(2,11).map(x => toDouble(x))
  val matched = pieces(11).toBoolean
  (id1,id2,scores,matched)
}
val tup = parse(line)

tup._1
tup.productElement(0)

tup.productArity

case class MatchData ( id1: Int, id2: Int, scores: Array[Double], matched: Boolean)

def parse ( line : String ) = {
val pieces = line.split(',')
val id1 = pieces(0).toInt
val id2 = pieces(1).toInt
val scores = pieces.slice(2,11).map(x => toDouble(x))
val matched = pieces(11).toBoolean
MatchData(id1,id2,scores,matched)
}
val md = parse(line)

md.matched
md.id1
md.scores

val mds = head.filter(!isHeader(_)).map(parse(_))

val parsed = noheader.map(line => parse(line))

//parsed.cache

val grouped = mds.groupBy(_.matched)

grouped.mapValues(x=>x.size).foreach(println)

val matchCounts = parsed.map(md => md.matched).countByValue()

val matchCountsSeq = matchCounts.toSeq

matchCountsSeq.sortBy(_._1).foreach(println)
matchCountsSeq.sortBy(_._2).foreach(println)

parsed.map(_.scores(0)).stats()

//import java.lang.Double.isNaN
parsed.map(_.scores(0)).filter(!_.isNaN).stats()

// !!! using stats as value name causes java.lang.NullPointerException for nasRDD !!!
val stats0to9 = (0 until 9).map(i => {
parsed.map(_.scores(i)).filter(!_.isNaN).stats()
})
stats0to9.foreach(println)

//:paste

//vi NAStatCounter.scala
 
import org.apache.spark.util.StatCounter

class NAStatCounter (d : Double) extends Serializable {
  val stats: StatCounter = new StatCounter()
  var missing: Long = 0
  def add(x: Double): NAStatCounter = {
    if (x.isNaN) {
      missing += 1
    } else {
      stats.merge(x)
    }
    this
  }
  def merge(other: NAStatCounter): NAStatCounter = {
    stats.merge(other.stats)
    missing += other.missing
    this
  }
  override def toString: String = {
    "stats: " + stats.toString + " NaN: " + missing
  }
  //constructor
  add(d)
}

object NAStatCounterFactory extends Serializable {
  def apply(d: Double) = new NAStatCounter(d)
}

//Ctrl+D

//:load NAStatCounter.scala

val nas1 = new NAStatCounter(10.0)
nas1.add(2.1)
nas1.add(1.2)
nas1.add(Double.NaN)
val nas2 = NAStatCounterFactory(Double.NaN)
nas2.add(1.0)
nas2.merge(nas1)

val arr = Array(1.0, Double.NaN, 17.29)
val nas = arr.map(NAStatCounterFactory(_))

val nasRDD0 = parsed.map(md => {
md.scores//.map(d => NAStatCounterFactory(d))
})
nasRDD0.first

val f = parsed.first
f.scores.map(d => NAStatCounterFactory(d))

val t10 = parsed.take(10)
t10.map( x => x.scores.map(d => NAStatCounterFactory(d)))

val nasRDD1 = parsed.map(md => {
md.scores.map(d => 1)//sc.parallelize(Array(d)).stats)
})
nasRDD1.first

val nasRDD = parsed.map(md => {
//md.scores.map(d => new NAStatCounter(d))
md.scores.map(d => NAStatCounterFactory(d))
})
nasRDD.first

val nas1 = Array(1.0,Double.NaN).map(d => NAStatCounterFactory(d))
val nas2 = Array(Double.NaN,2.0).map(d => NAStatCounterFactory(d))
val merged = nas1.zip(nas2).map(p => p._1.merge(p._2))

val merged = nas1.zip(nas2).map{ case (a,b) => a.merge(b)}

val nas = List(nas1,nas2)

val merged = nas.reduce((n1,n2) => {
n1.zip(n2).map{case(a,b) => a.merge(b)}
})

val reduced = nasRDD.reduce((n1, n2) => {
n1.zip(n2).map { case (a, b) => a.merge(b) }
})
reduced.foreach(println)

import org.apache.spark.rdd.RDD
def statsWithMissing(rdd: RDD[Array[Double]]): Array[NAStatCounter] = {
  val nastats = rdd.mapPartitions((iter: Iterator[Array[Double]]) => {
    val nas: Array[NAStatCounter] = iter.next().map(d => NAStatCounterFactory(d))
    iter.foreach(arr => {
      nas.zip(arr).foreach { case (n, d) => n.add(d) }
    })
    Iterator(nas)
  })
  nastats.reduce((n1, n2) => {
    n1.zip(n2).map { case (a, b) => a.merge(b) }
  })
}

val statsm = statsWithMissing(parsed.filter(_.matched).map(_.scores))
val statsn = statsWithMissing(parsed.filter(!_.matched).map(_.scores))

statsm.zip(statsn).map { case(m, n) =>
  (m.missing + n.missing, m.stats.mean - n.stats.mean)
}.foreach(println)

def naz(d: Double) = if (Double.NaN.equals(d)) 0.0 else d

case class Scored(md: MatchData, score: Double)

val ct = parsed.map(md => {
  val score = Array(2, 5, 6, 7, 8).map(i => naz(md.scores(i))).sum
  Scored(md, score)
})

ct.filter(s => s.score >= 4.0).
  map(s => s.md.matched).countByValue().foreach(println)

ct.filter(s => s.score >= 2.0).
  map(s => s.md.matched).countByValue().foreach(println)
