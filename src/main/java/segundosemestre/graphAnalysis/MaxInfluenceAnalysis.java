package segundosemestre.graphAnalysis;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class MaxInfluenceAnalysis implements Serializable {
    
    private static final long serialVersionUID = -6291209375768604631L;
    
    private class MaxComparator implements Serializable, Comparator<Tuple2<Integer,Double>> {
        
        private static final long serialVersionUID = -7410269533834544555L;

        @Override
        public int compare(Tuple2<Integer, Double> o1, Tuple2<Integer, Double> o2) {
            return Double.compare(o1._2(), o2._2());
        }
    }

    public void computeMaxInfluence(JavaPairRDD<Integer, Double> nodesInfluencePair) {
        JavaPairRDD<Integer, Double> nodesInfluenceReduced = reduceByKey(nodesInfluencePair);
        Tuple2<Integer, Double> maxInfluenceNode = getMaxInfluence(nodesInfluenceReduced);

        System.out.println("ID: " + maxInfluenceNode._1() + " | Influence: " + maxInfluenceNode._2());
    }
    
    private Tuple2<Integer, Double> getMaxInfluence(JavaPairRDD<Integer, Double> nodesInfluenceReduced) {
        return nodesInfluenceReduced.max(new MaxComparator());
    }
    
    private JavaPairRDD<Integer, Double> reduceByKey(JavaPairRDD<Integer, Double> nodesInfluencePair) {
        return nodesInfluencePair.reduceByKey(new Function2<Double, Double, Double>() {

            private static final long serialVersionUID = -7384522718223388271L;

            @Override
            public Double call(Double v1, Double v2) throws Exception {
                return v1 + v2;
            }
        });
    }
}
