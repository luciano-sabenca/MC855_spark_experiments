package segundosemestre.graphAnalysis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import segundosemestre.graphmaker.NodeInfluence;

public class MainGraphAnalysis {

    public static void main(String[] args) {
        AverageAnalysis average = new AverageAnalysis();
        MaxInfluenceAnalysis maxInfluence = new MaxInfluenceAnalysis();
        
        String dir = System.getProperty("user.dir");
        String graphState = dir + "/src/main/resources/mc855/lastGraph";

        SparkConf conf = new SparkConf().setAppName("MetricsGraph").setMaster("local[2]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<String> graphStateData = sparkContext.textFile(graphState);// .cache();
        JavaRDD<NodeInfluence> allNodesInfluence = toNodeInfluenceRdd(graphStateData);
        JavaPairRDD<Integer, Double> nodesInfluencePair = toPair(allNodesInfluence);
        
        average.computeAverage(nodesInfluencePair);
        maxInfluence.computeMaxInfluence(nodesInfluencePair);

        sparkContext.close();
    }

    private static JavaPairRDD<Integer, Double> toPair(JavaRDD<NodeInfluence> allNodesInfluence) {
        return allNodesInfluence.mapToPair(new PairFunction<NodeInfluence, Integer, Double>() {

            private static final long serialVersionUID = 4642931900100260790L;

            @Override
            public Tuple2<Integer, Double> call(NodeInfluence t) throws Exception {
                return new Tuple2<Integer, Double>(t.getSrc(), t.getInfluence());
            }
        });
    }
    
    private static JavaRDD<NodeInfluence> toNodeInfluenceRdd(JavaRDD<String> graphStateData) {
        return graphStateData.flatMap(new FlatMapFunction<String, NodeInfluence>() {

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
    }
}
