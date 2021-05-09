import java.util.Hashtable;
import java.util.Vector;

public class Bucket {
    //POS 0 PAGE POS 1 ROW [POS0,POS1]
    //if single secondary Vector of vectors sorted
    private Hashtable<String, Vector> keyValue;
    private int min;
    private int max;
    private int size;
}
