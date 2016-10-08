package segundosemestre.graphAnalysis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import segundosemestre.graphmaker.NodeInfluence;

public class MainGraphAnalysis {

    public static void main(String[] args) {
        String dir = System.getProperty("user.dir");
        String graphState = dir + "/src/main/resources/mc855/lastGraph";
        
        SparkConf conf = new SparkConf().setAppName("MetricsGraph").setMaster("local[2]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<String> graphStateData = sparkContext.textFile(graphState);//.cache();
        System.out.println(graphStateData.collect());
        
        //Get Node greatest influence
        JavaRDD<NodeInfluence> allNodesInfluence = graphStateData.map(new Function<String, NodeInfluence>() {

            private static final long serialVersionUID = 5651260484225642994L;

            @Override
            public NodeInfluence call(String v1) throws Exception {
                NodeInfluence influence = new NodeInfluence();
                Double totalInfluence = 0.0;
                
                String[] nodeEdges = v1.split(",");
                influence.setSrc(Integer.parseInt(nodeEdges[0].trim()));
                
                for(int i = 1; i < nodeEdges.length; i++) {
                    String[] edge = nodeEdges[i].split("->");
                    totalInfluence += Double.parseDouble(edge[1]);
                }
                
                influence.setInfluence(totalInfluence);
                
                return influence;
            }
        });
        
        NodeInfluence nodeMaxInfluence = allNodesInfluence.reduce(new Function2<NodeInfluence, NodeInfluence, NodeInfluence>() {
            
            private static final long serialVersionUID = -6235804602023425066L;

            @Override
            public NodeInfluence call(NodeInfluence v1, NodeInfluence v2) throws Exception {
                if(v1.getInfluence() > v2.getInfluence()) {
                    return v1;
                } else {
                    return v2;
                }
            }
        });
        
        /*NodeInfluence nodeMaxInfluence = allNodesInfluence.max(new Comparator<NodeInfluence>() {
            
            @Override
            public int compare(NodeInfluence o1, NodeInfluence o2) {
                if(o1.getInfluence() > o2.getInfluence()) {
                    return 1;
                } else {
                    return -1;
                }
            }
        });*/
        
        System.out.println("ID: " + nodeMaxInfluence.getSrc().toString() + " | Influence: " + nodeMaxInfluence.getInfluence().toString());
        
        sparkContext.close();
    }
}
