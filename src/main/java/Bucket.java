import java.io.FileInputStream;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class Bucket {
    //POS 0 PAGE POS 1 ROW [POS0,POS1]
    //if single secondary Vector of vectors sorted
    private Hashtable<Object, Vector> keyValue;
    private String tableName;
    private Object min;
    private Object max;
    private int size;

    public Bucket(String tableName) {
        this.keyValue = new Hashtable<Object,Vector>();
        this.tableName = tableName;
        int size=0;
    }

    public Hashtable<Object, Vector> getKeyValue() {
        return keyValue;
    }

    public void setSize(int count) {
        this.size = count;
    }

    public void incrementSize() {
        this.size++;
    }

    public void decrementSize() {
        this.size--;
    }

    public String getTableName() {
        return tableName;
    }

    public Object getMin() {
        return min;
    }

    public void setKeyValue(Hashtable<Object, Vector> keyValue) {
        this.keyValue = keyValue;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setMin(Object min) {
        this.min = min;
    }

    public void setMax(Object max) {
        this.max = max;
    }

    public Object getMax() {
        return max;
    }

    public int getSize() {
        return size;
    }
}
