import java.util.Hashtable;
import java.util.Vector;

public class Grid {
    // V,X
    private String[] columns;
    //key -> values--> <v1,X2>-->bucket
    private Hashtable<Vector, Vector<Bucket>> buckets;
    //[X,Y,Z...]
    private Hashtable<String,Object> range;
    private Hashtable<String,Object> min;
    private Hashtable<String,Object> max;
    private Hashtable<String,String> type;


    //multiple variations can point to the same bucket!

    public Grid(String[] columns, Hashtable<String, Object> min, Hashtable<String, Object> max, Hashtable<String, String> type) {
        this.columns = columns;
        this.buckets = new Hashtable();
        this.range = new Hashtable();
        this.min = min;
        this.max = max;
        this.type = type;
    }


    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }

}

//whenever we create a key we create a bucket with the range of values
