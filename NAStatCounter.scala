import org.apache.spark.util.StatCounter

class NAStatCounter (d : Double) extends Serializable{
  val stats: StatCounter = new StatCounter()
  var missing: Long = 0

  def add(x: Double): NAStatCounter = {
    if (java.lang.Double.isNaN(x)) {
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

  override def toString = {
    "stats:" + stats.toString() + "NaN:" + missing
  }
  
  add(d)
}

object NAStatCounterFactory extends Serializable {
  def apply(x: Double) = new NAStatCounter().add(x)
}

