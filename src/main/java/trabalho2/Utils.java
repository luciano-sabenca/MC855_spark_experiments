package trabalho2;

import scala.Tuple2;

/**
 * @author Luciano P. Sabenca (luciano.sabenca@movile.com)
 *         Creation Date: 23/04/16
 */
public class Utils {

    public static String toCsv(Tuple2<IP, Distance> tuple) {
        return tuple._1.ip + "," + tuple._1.latitude + "," + tuple._1.longitude + "," + tuple._2.cidade + "," + tuple._2.distance + "," + tuple._2.latitude
                + "," + tuple._2.longitute;

    }
}
