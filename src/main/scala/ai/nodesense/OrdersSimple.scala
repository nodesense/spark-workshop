package ai.nodesense

import org.apache.spark.{SparkContext, SparkConf}

case class Order(id : Int, productId: Int, price: Int);

object OrdersSimple extends  App {
  val conf: SparkConf = new SparkConf();
  conf.setMaster(s"local[1]");
  conf.setAppName("Order simple App");

  val sc = new SparkContext(conf);

  // Lazy load
  val inputfile = sc.textFile("src/main/resources/data/orders.csv");


  println("All lines ");

  inputfile.foreach(println(_));

  //val splitTokens = inputfile.flatMap( _.split(","))

  val orderData = inputfile.map( _.split(","))
      .map( x => (x(0).trim().toInt, x(1).trim().toInt, x(2).trim().toInt ) )
    .map (t => Order(t._1, t._2, t._3))


  orderData.foreach(println(_));



  val priceTotal = orderData.aggregate((0, 0)) (
    (acc, value) => (acc._1 + value.price, acc._2 + 1),
    (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
  )

  val priceTotal2 = orderData.map( x => x.price )
      .fold(0) ( (z, x) => z + x  )





  println("Price total", priceTotal)
  println("Price total", priceTotal2)



  //splitTokens.foreach(println(_));

  println("Count ", inputfile.count())

}
