package trabalho2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

/**
 * It uses a KDTree(https://en.wikipedia.org/wiki/K-d_tree) to divide the space and map each ip with the closest available location
 *
 * @author Luciano P. Sabenca (luciano.sabenca@movile.com)
 *         Creation Date: 15/04/16
 */
public class IPLocalizationOptimized {

    public static void main(String[] args) {
        String citiesInputFilename =
                System.getProperty("citiesInputFilename") != null ? System.getProperty("citiesInputFilename") : "src/main/resources/world_10567_cities.csv";
        String ipsInputFilename =
                System.getProperty("ipsInputFilename") != null ? System.getProperty("ipsInputFilename") : "src/main/resources/sample_1000000_ips.csv";
        String outputDir = System.getProperty("outputDir") != null ? System.getProperty("outputDir") : "optimized_localization";

        boolean prettyPrint = System.getProperty("prettyPrint") != null && Boolean.parseBoolean(System.getProperty("prettyPrint"));

        SparkConf conf = new SparkConf().setAppName("IPLocalizationOptimized").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<City> citiesList = new LinkedList<>();
        try (Stream<String> stream = Files.lines(Paths.get(citiesInputFilename))) {

            stream.forEach(line -> {
                City city = new City();
                String[] strings = line.split(";");

                city.name = strings[2];
                city.longitude = Double.parseDouble(strings[4]);
                city.latitude = Double.parseDouble(strings[3]);

                citiesList.add(city);
            });

        } catch (IOException e) {
            e.printStackTrace();
        }

        KDTree<City> tree = new KDTree<>(citiesList.size());
        citiesList.forEach(c -> tree.add(new double[] { c.latitude, c.longitude }, c));
        JavaRDD<String> ipsStr = sc.textFile(ipsInputFilename);

        JavaRDD<IP> ips = ipsStr.filter(l -> !l.startsWith("network") && l.split(",").length >= 9).map(txt -> {
            IP ip = new IP();
            String[] strings = txt.split(",");
            ip.ip = strings[0];
            ip.longitude = Double.parseDouble(strings[8]);
            ip.latitude = Double.parseDouble(strings[7]);
            return ip;
        });

        Broadcast<KDTree<City>> broadcast = sc.broadcast(tree);

        JavaPairRDD<IP, Distance> minDistances = ips.mapToPair((PairFunction<IP, IP, Distance>) ips1 -> {
            KDNode<City> node = broadcast.getValue().find_nearest(new double[] { ips1.latitude, ips1.longitude });
            City city = node.t;
            Double dist = Math.sqrt(Math.pow(city.latitude - ips1.latitude, 2) + Math.pow(city.longitude - ips1.longitude, 2));
            Distance distance = new Distance();
            distance.distance = dist;
            distance.cidade = city.name;
            distance.latitude = city.latitude;
            distance.longitute = city.longitude;
            return new Tuple2<>(ips1, distance);
        });

        if (!prettyPrint) {

            minDistances.map(Utils::toCsv).saveAsTextFile(outputDir);

        } else {
            minDistances.saveAsTextFile(outputDir);
        }

    }

}
