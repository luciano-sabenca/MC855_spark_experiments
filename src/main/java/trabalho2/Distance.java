package trabalho2;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.io.Serializable;

/**
 * @author Luciano P. Sabenca (luciano.sabenca@movile.com)
 *         Creation Date: 22/04/16
 */
public class Distance  implements Serializable{

    public String cidade;

    public Double distance;

    public Double latitude;

    public Double longitute;

    @Override
    public String toString() {
        return "Distance{" +
                "cidade='" + cidade + '\'' +
                ", distance=" + distance +
                ", latitude=" + latitude +
                ", longitute=" + longitute +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        Distance distance1 = (Distance) o;

        return new EqualsBuilder().append(cidade, distance1.cidade).append(distance, distance1.distance).append(latitude, distance1.latitude)
                                  .append(longitute, distance1.longitute).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(cidade).append(distance).append(latitude).append(longitute).toHashCode();
    }
}
