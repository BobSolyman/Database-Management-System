import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.Vector;

public class Page implements Serializable {
    private int noRows;
    private static int pageNo;
    private Vector tuples;
    private String table;
    private Object min;
    private Object max;
    private static int maxPage;

    public Page(String table) throws IOException {
        this.table=table;
        this.maxPage= readingFromConfigFile("MaximumRowsCountinPage");
    }


    public Object getMin() {
        return min;
    }

    public void setMin(Object min) {
        this.min = min;
    }

    public Object getMax() {
        return max;
    }

    public void setMax(Object max) {
        this.max = max;
    }

    public int getNoRows() {
        return noRows;
    }

    public void setNoRows(int noRows) {
        this.noRows = noRows;
    }

    public static void setMaxPage(int maxPage) {
        Page.maxPage = maxPage;
    }

    public static int getMaxPage() { return maxPage; }

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

    public String getTable() { return table; }

    public void setTable(String table) { this.table = table; }

    public int readingFromConfigFile(String string) throws IOException {
        Properties prop = new Properties();
        FileInputStream property = new FileInputStream("src/main/resources/DBApp.config");
        prop.load(property);
        return Integer.parseInt(prop.getProperty(string));
    }
}
