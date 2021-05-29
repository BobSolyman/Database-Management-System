import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;
import java.util.Date;

public class Grid implements Serializable {
    // V,X
    private Vector<String> columns;
    //key -> values--> <v1,X2>-->bucket
    private Hashtable<Vector, String> buckets;
    //[X,Y,Z...]
    private Hashtable<String,Object> range;
    private Hashtable<String,Object> min;
    private Hashtable<String,Object> max;
    private Hashtable<String,String> type;
    private String gridID ;

    //multiple variations can point to the same bucket!

    public Grid(String[] columns, Hashtable<String, Object> mi, Hashtable<String, Object> ma, Hashtable<String, String> t) {
        this.columns = new Vector<>();
        this.columns.addAll(Arrays.asList(columns));
        Collections.sort(this.columns);
        this.buckets = new Hashtable();
        this.range = new Hashtable();
        this.min = mi;
        this.max = ma;
        this.type = t;
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        gridID = timestamp.toString() ;

        for (String col : this.columns){
            if (((String)type.get(col)).equals("java.lang.Integer")){
                int step = ((int)max.get(col)-(int)min.get(col))/9;
                range.put(col,step);
            }
            else if (((String)type.get(col)).equals("java.lang.Double")){
                double step = ((double)max.get(col)-(double)min.get(col))/9;
                range.put(col,step);
            }
            else if (((String)type.get(col)).equals("java.util.Date")){
                int step = ((((Date)max.get(col)).getDate()-((Date)min.get(col)).getDate())/9);
                range.put(col,step);



            }else if (((String)type.get(col)).equals("java.lang.String")){
                // to be HANDLED
            }


        }

    }// END OF CONSTRUCTOR




    public Vector<Integer> getIndex (Record r){
        Vector <Integer> res = new Vector<>();

        for (String col : this.columns){

            if (((String)type.get(col)).equals("java.lang.Integer")){
                int step = (int)range.get(col);
                int i = 0 ;
                if(r.getContent().containsKey(col))
                     i = ((int)r.getContent().get(col)-(int)min.get(col))/step;
                else
                     i = -1 ;
                res.add(i);
            }
            else if (((String)type.get(col)).equals("java.lang.Double")){
                double step = (double)range.get(col);
                double i = 0 ;
                if(r.getContent().containsKey(col))
                    i = ((double)r.getContent().get(col)-(double)min.get(col))/step;
                else
                    i = -1 ;
                res.add((int)i);

            }
            else if (((String)type.get(col)).equals("java.util.Date")){
                int step = (int)range.get(col);
                int i = 0 ;
                if(r.getContent().containsKey(col))
                    i = (((Date)r.getContent().get(col)).getDate()-((Date)min.get(col)).getDate())/step;
                else
                    i = -1;
                res.add(i);

            }else if (((String)type.get(col)).equals("java.lang.String")){
                // to be HANDLED

            }


        }

     return res ;
    }

    public String getGridID() {
        return gridID;
    }

    public Vector<String> getColumns() {
        return columns;
    }

    public void setColumns(Vector<String> columns) {
        this.columns = columns;
    }

    public Hashtable<Vector, String> getBuckets() {
        return buckets;
    }

    public void setBuckets(Hashtable<Vector, String> buckets)
    {
        this.buckets = buckets;
    }

    public Hashtable<String, Object> getRange() {
        return range;
    }

    public void setRange(Hashtable<String, Object> range) {
        this.range = range;
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
}

//whenever we create a key we create a bucket with the range of values
