package segundosemestre.graphAnalysis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import segundosemestre.graphmaker.NodeInfluence;

public class MainGraphAnalysis {

    public static void main(String[] args) {
        String dir = System.getProperty("user.dir");
        String graphState = dir + "/src/main/resources/mc855/lastGraph";
        
        SparkConf conf = new SparkConf().setAppName("MetricsGraph").setMaster("local[2]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<String> graphStateData = sparkContext.textFile(graphState);//.cache();
        System.out.println(graphStateData.collect());
        
        JavaRDD<NodeInfluence> allNodesInfluence2 = graphStateData.flatMap(new FlatMapFunction<String, NodeInfluence>() {

            private static final long serialVersionUID = 2438252684892282708L;

            @Override
            public Iterator<NodeInfluence> call(String t) throws Exception {
                List<NodeInfluence> influences = new ArrayList<NodeInfluence>();
                
                String[] nodeEdges = t.split(",");
                
                for(int i = 1; i < nodeEdges.length; i++) {
                    NodeInfluence influence = new NodeInfluence();
                    influence.setSrc(Integer.parseInt(nodeEdges[0].trim()));
                    
                    String[] edge = nodeEdges[i].split("->");
                    influence.setDst(Integer.parseInt(edge[0].trim()));
                    influence.setInfluence(Double.parseDouble(edge[1].trim()));
                    
                    influences.add(influence);
                }
                
                return influences.iterator();
            }
        });
        
       JavaPairRDD<Integer, Double> nodesInfluencePair = allNodesInfluence2.mapToPair(new PairFunction<NodeInfluence, Integer, Double>() {

            private static final long serialVersionUID = 4642931900100260790L;

            @Override
            public Tuple2<Integer, Double> call(NodeInfluence t) throws Exception {
                return new Tuple2<Integer, Double>(t.getSrc(), t.getInfluence());
            }
        });
       
       JavaPairRDD<Integer, Double> nodesInfluenceReduced = nodesInfluencePair.reduceByKey(new Function2<Double, Double, Double>() {
           
        private static final long serialVersionUID = -7384522718223388271L;

            @Override
            public Double call(Double v1, Double v2) throws Exception {
                return v1 + v2;
            }
        });
       
       Tuple2<Integer, Double> maxInfluenceNode = nodesInfluenceReduced
                .reduce(new Function2<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {

                    private static final long serialVersionUID = -1151045189581289744L;

                    @Override
                    public Tuple2<Integer, Double> call(Tuple2<Integer, Double> v1, Tuple2<Integer, Double> v2)
                            throws Exception {
                        if(Double.compare(v1._2(), v2._2()) > 0) {
                            return v1;
                        } else {
                            return v2;
                        }
                    }
                });
       
       System.out.println("ID: " + maxInfluenceNode._1() + " | Influence: " + maxInfluenceNode._2());
        
        sparkContext.close();
    }
}
