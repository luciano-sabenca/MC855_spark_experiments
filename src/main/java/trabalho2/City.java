package trabalho2;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.io.Serializable;

/**
 * @author Luciano P. Sabenca (luciano.sabenca@movile.com)
 *         Creation Date: 23/04/16
 */
public class City implements Serializable {
    String name;
    Double longitude;
    Double latitude;

    @Override
    public String toString() {
        return "City{" +
                "name='" + name + '\'' +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        City city = (City) o;

        return new EqualsBuilder().append(name, city.name).append(longitude, city.longitude).append(latitude, city.latitude).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(name).append(longitude).append(latitude).toHashCode();
    }
}
