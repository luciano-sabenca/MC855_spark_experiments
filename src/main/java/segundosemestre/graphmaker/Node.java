package segundosemestre.graphmaker;

import java.util.HashMap;
import java.util.Map;

public class Node {

    private Integer id;
    private Map<Integer, Double> state = new HashMap<>();

    public Node() {
    }

    public Node(Integer id) {
        this.id = id;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Map<Integer, Double> getState() {
        return state;
    }

    public void setState(Map<Integer, Double> state) {
        this.state = state;
    }
}
