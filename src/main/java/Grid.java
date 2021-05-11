import java.util.Hashtable;
import java.util.Vector;

public class Grid {
    // V,X
    private String[] columns;
    //key -> values--> <v1,X2>-->bucket
    private Hashtable<Vector, Vector<Bucket>> buckets;
    //multiple variations can point to the same bucket!

    public Grid(String[] columns, Hashtable<Vector, Vector<Bucket>> buckets) {
        this.columns = columns;
        this.buckets = buckets;
    }

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }

}

//whenever we create a key we create a bucket with the range of values
