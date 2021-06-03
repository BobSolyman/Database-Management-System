import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class Bucket implements Serializable {
    //POS 0 PAGE POS 1 ROW [POS0,POS1]
    //if single secondary Vector of vectors sorted
    private Vector<bucketEntry> entries ;
    private static int maxBucket;
    private Hashtable<String,Object> min;
    private Hashtable<String,Object> max;
    private Hashtable<String,String> type;
    private Vector<String> columns;



    public Bucket(Hashtable<String, String> type, Vector<String> columns) throws IOException {
        this.entries = new Vector<bucketEntry>();
        this.type = type;
        this.min = new Hashtable<>();
        this.max = new Hashtable<>();
        this.columns = columns;
        this.maxBucket= Page.readingFromConfigFile("MaximumKeysCountinIndexBucket");
    }

    public void updateMinMax (bucketEntry x){
        // use it every time with insertion
        if(x==null){
            for (bucketEntry bE: this.getEntries()){
                updateMinMax(bE);
            }
        }
        else {
            Hashtable<String, Object> r = x.getRow().getContent();
            if (min.size()==0){
                for (Map.Entry p : r.entrySet()){
                    min.put((String) p.getKey(), p.getValue());
                    max.put((String) p.getKey(), p.getValue());
                }
                return;
            }

            for (Map.Entry m : r.entrySet()) {
                if (columns.contains(m.getKey())) {
                    if (((String) type.get(m.getKey())).equals("java.lang.Integer")) {
                        if ((Integer) min.get(m.getKey()) > (Integer) m.getValue()) {
                            min.put((String) m.getKey(), m.getValue());
                        }
                        if ((Integer) max.get(m.getKey()) < (Integer) m.getValue()) {
                            max.put((String) m.getKey(), m.getValue());
                        }
                    } else if (((String) type.get(m.getKey())).equals("java.lang.Double")) {
                        if ((Double) min.get(m.getKey()) > (Double) m.getValue()) {
                            min.put((String) m.getKey(), m.getValue());
                        }
                        if ((Double) max.get(m.getKey()) < (Double) m.getValue()) {
                            max.put((String) m.getKey(), m.getValue());
                        }
                    } else if (((String) type.get(m.getKey())).equals("java.util.Date")) {
                        if (((Date) min.get(m.getKey())).compareTo((Date) m.getValue()) > 0) {
                            min.put((String) m.getKey(), m.getValue());
                        }
                        if (((Date) max.get(m.getKey())).compareTo((Date) m.getValue()) < 0) {
                            max.put((String) m.getKey(), m.getValue());
                        }


                    } else if (((String) type.get(m.getKey())).equals("java.lang.String")) {
                        if (((String) min.get(m.getKey())).compareTo((String) m.getValue()) > 0) {
                            min.put((String) m.getKey(), m.getValue());
                        }
                        if (((String) max.get(m.getKey())).compareTo((String) m.getValue()) < 0) {
                            max.put((String) m.getKey(), m.getValue());
                        }
                    }
                }
            }
        }
    }

    public int searchBucketEntry(bucketEntry bE){
        int i = 0;



        Comparator<bucketEntry> c = new Comparator<bucketEntry>() {
            public int compare(bucketEntry u1, bucketEntry u2)
            {
                for(String col : columns) {
                    String t = type.get(col);
                    if (t.equals("java.lang.Integer")) {

                        Integer min1 = (int) u1.getRow().getContent().get(col);
                        Integer min2 = (int) u2.getRow().getContent().get(col);
                        if(min1.compareTo(min2)==0){
                            continue;
                        }
                        return min1.compareTo(min2);
                    } else if (t.equals("java.lang.Double")) {
                        Double min1 = (double) u1.getRow().getContent().get(col);
                        Double min2 = (double) u2.getRow().getContent().get(col);
                        if(min1.compareTo(min2)==0){
                            continue;
                        }
                        return min1.compareTo(min2);
                    } else if (t.equals("java.util.Date")) {
                        Date min1 = (Date) u1.getRow().getContent().get(col);
                        Date min2 = (Date) u2.getRow().getContent().get(col);
                        if(min1.compareTo(min2)==0){
                            continue;
                        }
                        return min1.compareTo(min2);
                    } else {
                        String min1 = (String) u1.getRow().getContent().get(col);
                        String min2 = (String) u2.getRow().getContent().get(col);
                        if(min1.compareTo(min2)==0){
                            continue;
                        }
                        return min1.compareTo(min2);

                    }
                }
                return 0;

            }};
        i = Collections.binarySearch(this.getEntries(),bE,c);
        if (i == -1){
            i = 0;
        }
        else if (i < 0){
            i = Math.abs(i+1)-1;

        }



        return i;
    }


    public Hashtable<String, Object> getMin() {
        return min;
    }

    public void setMin(Hashtable<String, Object> min) {
        this.min = min;
    }

    public Hashtable<String, Object> getMax() {
        return max;
    }

    public void setMax(Hashtable<String, Object> max) {
        this.max = max;
    }

    public Hashtable<String, String> getType() {
        return type;
    }

    public void setType(Hashtable<String, String> type) {
        this.type = type;
    }

    public Vector<bucketEntry> getEntries() {
        return entries;
    }

    public void setEntries(Vector<bucketEntry> entries) {
        this.entries = entries;
    }

    public int getMaxBucket() {
        return maxBucket;
    }

    public Vector<String> getColumns() {
        return columns;
    }

    public void setColumns(Vector<String> columns) {
        this.columns = columns;
    }
}
