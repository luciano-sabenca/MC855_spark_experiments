package segundosemestre.graphAnalysis;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;

public class AverageAnalysis implements Serializable {
 
    private static final long serialVersionUID = -1150133350571889504L;

    public static class AvgCount implements Serializable {
        private static final long serialVersionUID = 8775376041607726641L;
        public double totalSum;
        public int count;
        
        public AvgCount(int total, int num) {
            totalSum = total;
            count = num;
        }

        public double avg() {
            return totalSum / (float) count;
        }
        
        @Override
        public String toString(){
            return Double.toString(avg());
        }
    }
    
    public void computeAverage(JavaPairRDD<Integer, Double> nodesInfluencePair) {
        Function2<AvgCount, Double, AvgCount> addAndCount = getWithinPartitionReductionFunc();
        
        Function2<AvgCount, AvgCount, AvgCount> combine = getCrossPartitionReductionFunc();
        
        AvgCount initial = getInitialAvgCount();

        JavaPairRDD<Integer, AvgCount> averagePerNode = nodesInfluencePair.aggregateByKey(initial, addAndCount, combine);

        System.out.println("Collecting: " + averagePerNode.collect());
    }
    
    private AvgCount getInitialAvgCount() {
        return new AvgCount(0, 0);
    }
    
    private Function2<AvgCount, AvgCount, AvgCount> getCrossPartitionReductionFunc() {
        return new Function2<AvgCount, AvgCount, AvgCount>() {
            private static final long serialVersionUID = 9086472550068350956L;

            @Override
            public AvgCount call(AvgCount a, AvgCount b) {
                a.totalSum += b.totalSum;
                a.count += b.count;
                return a;
            }
        };
    }
    
    private Function2<AvgCount, Double, AvgCount> getWithinPartitionReductionFunc() {
        return new Function2<AvgCount, Double, AvgCount>() {
            private static final long serialVersionUID = 7236225295138349618L;

            @Override
            public AvgCount call(AvgCount a, Double x) {
                a.totalSum += x;
                a.count += 1;
                return a;
            }
        };
    }
}
