import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.*;

public class DBTable<T extends Comparable <T>> implements Serializable {
    //contains pages within this table in form [location,upperBound,LowerBound,TableName,NoTuples]
    private Vector<Vector<T>> pages;
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
        this.pages = new Vector<>();
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

    public int searchPage (Record r){
       int i = 0 ;

       Vector v = new Vector();
       v.add("Somewhere");
       v.add(r.getData().get(0).getValue());
       v.add(r.getData().get(0).getValue());
       String type = colNameType.get((String)clusteringKey);
        Comparator<Vector> c = new Comparator<Vector>() {
            public int compare(Vector u1, Vector u2)
            {
                if (type.equals("java.lang.Integer")){

                Integer min1 = (int)u1.get(2);
                Integer min2 = (int)u2.get(2);
                return min1.compareTo(min2);
                }
                else if (type.equals("java.lang.Double")){
                    Double min1 = (double)u1.get(2);
                    Double min2 = (double)u2.get(2);
                    return min1.compareTo(min2);
                }
                else if (type.equals("java.util.Date")){
                    Date min1 = (Date)u1.get(2);
                    Date min2 = (Date)u2.get(2);
                    return min1.compareTo(min2);
                }
                else {
                    String min1 = (String)u1.get(2);
                    String min2 = (String)u2.get(2);
                    return min1.compareTo(min2);

                }

            }};
        i = Collections.binarySearch(this.getPages(),v,c);
        if (i == -1){
            i = 0;
        }
        else if (i < 0){
            i = Math.abs(i+1)-1;

        }


    return i ;
    }// end of method

    public Vector<Vector<T>> getPages() {
        return pages;
    }

    public void setPages(Vector<Vector<T>> pages) {
        this.pages = pages;
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
