import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class vecBucket implements Serializable {

    private  String bucketID ;
    private Vector<Bucket> Buckets ;
    private Vector<String> columns;
    private Hashtable<String,String> type;

    public Hashtable<String, String> getType() {
        return type;
    }

    public void setType(Hashtable<String, String> type) {
        this.type = type;
    }

    public vecBucket(String bucketID, Vector<String> columns, Hashtable<String,String> type) {
        this.bucketID = bucketID;
        Buckets = new Vector<Bucket>();
        this.columns = columns;
        this.type = type;
    }

    public Vector<String> getColumns() {
        return columns;
    }

    public void setColumns(Vector<String> columns) {
        this.columns = columns;
    }

    public String getBucketID() {
        return bucketID;
    }


    public Vector<Bucket> getBuckets() {
        return Buckets;
    }

    public void setBuckets(Vector<Bucket> buckets) {
        Buckets = buckets;
    }

    public int searchBuckets(bucketEntry bE){
        int i = 0;
        Bucket b = null;
        try {
            b = new Bucket(type, columns);
        } catch (IOException e) {
            e.printStackTrace();
        }
        b.getEntries().add(bE);
        b.updateMinMax(bE);

        Comparator<Bucket> c = new Comparator<Bucket>() {
            public int compare(Bucket u1, Bucket u2)
            {
                for(String col : columns) {
                    String t = type.get(col);
                    if (t.equals("java.lang.Integer")) {

                        Integer min1 = (int) u1.getMin().get(col);
                        Integer min2 = (int) u2.getMin().get(col);
                        if(min1.compareTo(min2)==0){
                            continue;
                        }
                        return min1.compareTo(min2);
                    } else if (t.equals("java.lang.Double")) {
                        Double min1 = (double) u1.getMin().get(col);
                        Double min2 = (double) u2.getMin().get(col);
                        if(min1.compareTo(min2)==0){
                            continue;
                        }
                        return min1.compareTo(min2);
                    } else if (t.equals("java.util.Date")) {
                        Date min1 = (Date) u1.getMin().get(col);
                        Date min2 = (Date) u2.getMin().get(col);
                        if(min1.compareTo(min2)==0){
                            continue;
                        }
                        return min1.compareTo(min2);
                    } else {
                        String min1 = (String) u1.getMin().get(col);
                        String min2 = (String) u2.getMin().get(col);
                        if(min1.compareTo(min2)==0){
                            continue;
                        }
                        return min1.compareTo(min2);

                    }
                }
                return 0;

            }};
        i = Collections.binarySearch(this.getBuckets(),b,c);
        if (i == -1){
            i = 0;
        }
        else if (i < 0){
            i = Math.abs(i+1)-1;

        }



        return i;
    }
}
