package segundosemestre

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, _}
import org.apache.spark.rdd.RDD
import segundosemestre.graphmaker.NodeInfluence

/**
  * @author Luciano P. Sabenca (luciano.sabenca@movile.com)
  *         Creation Date: 09/10/16
  */
object GraphMaker {

  val r = scala.util.Random


  def createNodes(sparkContext: SparkContext, qtd: Int): RDD[(VertexId, Map[VertexId, Double])] = {
    sparkContext.parallelize(Range(1, qtd).map((x: Int) => {((x: VertexId), Map[VertexId, Double]())} : (VertexId, Map[VertexId, Double])))
  }


  def createEdges(sparkContext: SparkContext, qtd: Int, maxNode: Int): RDD[Edge[Double]] = {
    sparkContext parallelize Range(1, qtd).map(_ => {
      val a = r.nextInt(maxNode)
      val b = r.nextInt(maxNode)
      if (a != b) {
        Edge(a, b, r.nextDouble())
      } else {
        Edge(a, r.nextInt(maxNode), r.nextDouble())
      }
    })
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Simple Application")

    val sc = new SparkContext(conf)

    val MAX_NODES = 10
    val MAX_EDGES = 50

    val graph = Graph(createNodes(sc, MAX_NODES), createEdges(sc, MAX_EDGES, MAX_NODES))

    graph.pageRank(100).vertices.foreach(println)



  }



}
