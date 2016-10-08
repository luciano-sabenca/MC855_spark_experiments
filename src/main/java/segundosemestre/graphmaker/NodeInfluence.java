package segundosemestre.graphmaker;

import java.io.Serializable;

public class NodeInfluence implements Serializable, Comparable<NodeInfluence> {

    private Integer src;
    private Integer dst;

    private Double influence;

    public NodeInfluence(){

    }

    public NodeInfluence(Integer dst, Integer src, Double influence) {
        this.dst = dst;
        this.src = src;
        this.influence = influence;
    }

    public Integer getSrc() {
        return src;
    }

    public void setSrc(Integer src) {
        this.src = src;
    }

    public Integer getDst() {
        return dst;
    }
    
    public void setDst(Integer dst) {
        this.dst = dst;
    }

    public Double getInfluence() {
        return influence;
    }

    public void setInfluence(Double influence) {
        this.influence = influence;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof NodeInfluence))
            return false;

        NodeInfluence that = (NodeInfluence) o;

        if (src != null ? !src.equals(that.src) : that.src != null)
            return false;
        return dst != null ? !dst.equals(that.dst) : that.dst != null;

    }

    @Override
    public int hashCode() {
        int result = src != null ? src.hashCode() : 0;
        result = 31 * result + (dst != null ? dst.hashCode() : 0);
        result = 31 * result + (influence != null ? influence.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(NodeInfluence o) {
        return this.equals(o)? 0 : this.dst - o.dst;
    }

    @Override
    public String toString() {
        return dst + "->" + influence;
    }
}