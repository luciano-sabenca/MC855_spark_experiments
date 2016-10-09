package segundosemestre.graphAnalysis;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class MaxInfluenceAnalysis implements Serializable {
    
    private static final long serialVersionUID = -6291209375768604631L;

    public void computeMaxInfluence(JavaPairRDD<Integer, Double> nodesInfluencePair) {
        JavaPairRDD<Integer, Double> nodesInfluenceReduced = reduceByKey(nodesInfluencePair);
        Tuple2<Integer, Double> maxInfluenceNode = getMaxInfluence(nodesInfluenceReduced);

        System.out.println("ID: " + maxInfluenceNode._1() + " | Influence: " + maxInfluenceNode._2());
    }
    
    private Tuple2<Integer, Double> getMaxInfluence(JavaPairRDD<Integer, Double> nodesInfluenceReduced) {
        return nodesInfluenceReduced
                .reduce(new Function2<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {

                    private static final long serialVersionUID = -1151045189581289744L;

                    @Override
                    public Tuple2<Integer, Double> call(Tuple2<Integer, Double> v1, Tuple2<Integer, Double> v2)
                            throws Exception {
                        if (Double.compare(v1._2(), v2._2()) > 0) {
                            return v1;
                        } else {
                            return v2;
                        }
                    }
                });
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
