package trabalho2;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.io.Serializable;

/**
 * @author Luciano P. Sabenca (luciano.sabenca@movile.com)
 *         Creation Date: 23/04/16
 */
public class IP implements Serializable {

    String ip;
    Double latitude;
    Double longitude;


    @Override
    public String toString() {
        return "IP{" +
                "ip='" + ip + '\'' +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        IP ip1 = (IP) o;

        return new EqualsBuilder().append(ip, ip1.ip).append(longitude, ip1.longitude).append(latitude, ip1.latitude).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(ip).append(longitude).append(latitude).toHashCode();
    }
}
