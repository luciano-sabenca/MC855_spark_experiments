import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphInfluence {

  def mergeMapsAndSumValues(m1: Map[Long, Double], m2: Map[Long, Double]): Map[Long, Double] = {
    m1 ++ m2.map { case (k, v) => k -> (v + m1.getOrElse(k, 0.0)) }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Simple Application")

    val sc = new SparkContext(conf)

    val vertices: RDD[(VertexId, Map[VertexId, Double])] = sc.parallelize(
      Array((1L, Map[VertexId, Double]()),
        (2L, Map[VertexId, Double]()),
        (3L, Map[VertexId, Double]()),
        (4L, Map[VertexId, Double]())))


    val edges: RDD[Edge[Double]] = sc.parallelize(Array(
      Edge(1, 2, 0.3), Edge(2, 3, 0.5), Edge(4, 3, 0.5)))

    val graph = Graph(vertices, edges)


    val v = graph.pregel(Map[Long, Double](), 30)(
      (id, v, m) => mergeMapsAndSumValues(v, m),
      triplet => {
        Iterator((triplet.dstId, Map[Long, Double](triplet.srcId -> triplet.attr)),
          (triplet.srcId, Map[Long, Double](triplet.srcId -> 0))) //  Writing to the same vertex to force it
      },
      (a, b) => mergeMapsAndSumValues(a, b)
    )


    println(v.vertices.collect().mkString("\n"))
  }


}