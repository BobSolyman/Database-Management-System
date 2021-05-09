import java.util.Hashtable;
import java.util.Vector;

public class Grid {
    // V,X
    private Vector Columns;
    //key -> values--> <v1,X2>-->bucket
    private Hashtable<Vector,Bucket> buckets;
    //multiple variations can point to the same bucket!
}

//whenever we create a key we create a bucket with the range of values
