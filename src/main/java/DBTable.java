import java.io.Serializable;
import java.util.Hashtable;

public class DBTable implements Serializable {

    private int noPages;
    private String name;
    private String clusteringKey;
    private Hashtable<String, String> colNameType;
    private Hashtable<String, String> colNameMin;
    private Hashtable<String, String> colNameMax;


    public DBTable(String name,String clusteringKey){
        this.name = name;
        this.clusteringKey=clusteringKey;
    }

    public void setColNameType(Hashtable<String, String> colNameType) {
        this.colNameType = colNameType;
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

    public Hashtable<String, String> getColNameType() {
        return colNameType;
    }

    public Hashtable<String, String> getColNameMin() {
        return colNameMin;
    }

    public Hashtable<String, String> getColNameMax() {
        return colNameMax;
    }
}
