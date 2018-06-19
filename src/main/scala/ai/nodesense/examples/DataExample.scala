package ai.nodesense.examples


import scala.collection.JavaConversions._
import scala.util.Random

object ScalaData {

  val r = new Random()
  def sampleData() : Seq[Customer] =
    (1 to 10)
    .map(i => {

      Customer(i, "First", "Last", 18 + r.nextInt(7), "KA")
    })
}

case class Customer(id: Int, first: String, last: String, age: Int, state: String)