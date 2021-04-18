import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class DBTable implements Serializable {

    private int noPages;
    private String name;
    private String clusteringKey;
    private Vector tuples;
    private Hashtable<String, String> colNameMin;
    private Hashtable<String, String> colNameMax;


    public DBTable(String name,String clusteringKey){
        this.name = name;
        this.clusteringKey=clusteringKey;
    }

    public void setNoPages(int noPages) {
        this.noPages = noPages;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setClusteringKey(String clusteringKey) {
        this.clusteringKey = clusteringKey;
    }

    public Vector getTuples() {
        return tuples;
    }

    public void setTuples(Vector tuples) {
        this.tuples = tuples;
    }

    public void setColNameMin(Hashtable<String, String> colNameMin) {
        this.colNameMin = colNameMin;
    }

    public void setColNameMax(Hashtable<String, String> colNameMax) {
        this.colNameMax = colNameMax;
    }

    public int getNoPages() {
        return noPages;
    }

    public String getName() {
        return name;
    }

    public String getClusteringKey() {
        return clusteringKey;
    }



    public Hashtable<String, String> getColNameMin() {
        return colNameMin;
    }

    public Hashtable<String, String> getColNameMax() {
        return colNameMax;
    }
}
