import java.io.Serializable;
import java.util.Vector;

public class Page implements Serializable {
    private static int noRows;
    private static int pageNo;
    private Vector tuples;
    private DBTable table;

    public Page(int noRows,DBTable table){
        this.noRows=noRows;
        this.table=table;
    }

    public static int getNoRows() {
        return noRows;
    }

    public static int getPageNo() {
        return pageNo;
    }

    public static void setPageNo(int pageNo) {
        Page.pageNo = pageNo;
    }

    public Vector getTuples() {
        return tuples;
    }

    public void setTuples(Vector tuples) {
        this.tuples = tuples;
    }

    public DBTable getTable() {
        return table;
    }

    public void setTable(DBTable table) {
        this.table = table;
    }
}
