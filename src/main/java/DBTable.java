import java.io.Serializable;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

public class DBTable implements Serializable {

    private Vector pages;
    private String name;
    private String clusteringKey;
    private Vector tuples;
    private Hashtable<String, String> colNameMin;
    private Hashtable<String, String> colNameMax;
    private Hashtable<String, String> colNameType;



    public DBTable(String name, String clusteringKey,Hashtable<String,String> colNameType){
        this.name = name;
        this.clusteringKey=clusteringKey;
        this.colNameType = colNameType;
    }

    public void addColumn(String name, String type, String min, String max){
        this.colNameType.put(name,type);
        this.colNameMin.put(name,min);
        this.colNameMax.put(name,max);
    }

    public void displayAttributes(){
        System.out.println("Name: "+this.name +" ck: "+ this.clusteringKey);
        Set<String> keys = this.getColNameType().keySet();
        for(String key: keys){
            String value = this.getColNameType().get(key);
            System.out.println("===="+key+": "+value);
        }
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


    public String getName() {
        return name;
    }

    public String getClusteringKey() {
        return clusteringKey;
    }

    public Hashtable<String, String> getColNameType() {
        return colNameType;
    }

    public void setColNameType(Hashtable<String, String> colNameType) {
        this.colNameType = colNameType;
    }

    public Hashtable<String, String> getColNameMin() {
        return colNameMin;
    }

    public Hashtable<String, String> getColNameMax() {
        return colNameMax;
    }
}
