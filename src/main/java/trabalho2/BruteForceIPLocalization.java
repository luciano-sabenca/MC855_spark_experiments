package trabalho2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Luciano P. Sabenca (luciano.sabenca@movile.com)
 *         Creation Date: 23/04/16
 */
public class BruteForceIPLocalization {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("BruteForceIPLocalization").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String citiesInputFilename = System.getProperty("citiesInputFilename") != null ?
                System.getProperty("citiesInputFilename") :
                "src/main/resources/world_sample_5000_cities.csv";
        String ipsInputFilename =
                System.getProperty("ipsInputFilename") != null ? System.getProperty("ipsInputFilename") : "src/main/resources/sample_10000_ips.csv";
        String outputDir = System.getProperty("outputDir") != null ? System.getProperty("outputDir") : "brute_force_localization";
        boolean prettyPrint = System.getProperty("prettyPrint") != null && Boolean.parseBoolean(System.getProperty("prettyPrint"));

        JavaRDD<String> citiesStr = sc.textFile(citiesInputFilename);
        JavaRDD<City> cities = citiesStr.map(line -> {
            City city = new City();
            String[] strings = line.split(";");

            city.name = strings[2];
            city.longitude = Double.parseDouble(strings[4]);
            city.latitude = Double.parseDouble(strings[3]);
            return city;

        });

        JavaRDD<String> ipsStr = sc.textFile(ipsInputFilename);

        JavaRDD<IP> ips = ipsStr.filter(l -> !l.startsWith("network")).map(txt -> {
            IP ip = new IP();
            String[] strings = txt.split(",");
            ip.ip = strings[0];
            ip.longitude = Double.parseDouble(strings[8]);
            ip.latitude = Double.parseDouble(strings[7]);
            return ip;
        });

        JavaPairRDD<IP, City> cartesian = ips.cartesian(cities);

        JavaPairRDD<IP, Iterable<Distance>> distances = cartesian.groupByKey().mapToPair(
                (PairFunction<Tuple2<IP, Iterable<City>>, IP, Iterable<Distance>>) cidadeIterableTuple2 -> {
                    IP ip = cidadeIterableTuple2._1;
                    List<Distance> distances1 = new ArrayList<>();
                    for (City city : cidadeIterableTuple2._2) {
                        Double dist = Math.sqrt(Math.pow(city.latitude - ip.latitude, 2) + Math.pow(city.longitude - ip.longitude, 2));
                        Distance distance = new Distance();
                        distance.cidade = city.name;
                        distance.distance = dist;
                        distance.latitude = city.latitude;
                        distance.longitute = city.longitude;
                        distances1.add(distance);

                    }
                    return new Tuple2<>(ip, distances1);

                });

        JavaPairRDD<IP, Distance> minDistances = distances.mapToPair((PairFunction<Tuple2<IP, Iterable<Distance>>, IP, Distance>) ipIterableTuple2 -> {
            Iterable<Distance> distances1 = ipIterableTuple2._2;
            Distance min = null;

            for (Distance distance : distances1) {
                if (min == null) {
                    min = distance;
                }
                if (min.distance.compareTo(distance.distance) > 0) {
                    min = distance;
                }

            }

            return new Tuple2<>(ipIterableTuple2._1, min);

        });

        if (!prettyPrint) {
            minDistances.map(Utils::toCsv).saveAsTextFile(outputDir);
        } else {
            minDistances.saveAsTextFile(outputDir);
        }

    }

}


