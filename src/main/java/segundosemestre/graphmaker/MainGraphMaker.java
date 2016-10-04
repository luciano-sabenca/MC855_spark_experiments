package segundosemestre.graphmaker;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import org.graphframes.lib.PageRank;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MainGraphMaker {

    private static final int N_NODES = 10;

    public static void main(String[] args) {
        String outputDir = System.getProperty("outputDir") != null ? System.getProperty("outputDir") : "optimized_localization";

        SparkSession spark = SparkSession.builder()
                .appName("SparkGraphFrameSample")
                .config("spark.sql.warehouse.dir", "/file:/temp")
                .master("local[2]")
                .getOrCreate();

        GraphFrame graph = createGraph(spark);

        graph.inDegrees().show();

        long count = graph.edges().filter("influence > 0.5").count();
        System.out.println("influence > 0.5: " + count);

        PageRank pRank = graph.pageRank().resetProbability(0.01).maxIter(5);
        pRank.run().vertices().select("id", "pagerank").show();

        spark.stop();
    }

    private static GraphFrame createGraph(SparkSession spark) {
        Dataset<Row> nodesDF = spark.createDataFrame(createNodes(), Node.class);
        Dataset<Row> edgDF = spark.createDataFrame(createEdges(), NodeInfluence.class);

        return new GraphFrame(nodesDF, edgDF);
    }

    private static List<NodeInfluence> createEdges() {
        Set<NodeInfluence> rList = new HashSet<>();
        for (int i = 0; i < N_NODES * N_NODES; i++) {
            int source = getRandomInt();
            int destination = getRandomInt();
            if (source != destination) {
                rList.add(new NodeInfluence(source, destination, Math.random()));
            }
        }
        return new ArrayList<>(rList);
    }

    private static List<Node> createNodes() {
        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < N_NODES; i++) {
            nodes.add(new Node(i));
        }
        return nodes;
    }

    private static int getRandomInt() {
        return (int) (Math.random() * N_NODES);
    }
}
