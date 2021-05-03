import java.io.Serializable;
import java.util.Date;

public class Pair implements Serializable, Comparable {
   private String key ;
   private Object value ;

    public String getKey() {
        return key;
    }


    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Pair(String x, Object y) {
        this.key = x;
        this.value = y;
    }

    @Override
    public String toString() {
        return "{"
                + key + '\''
                + value +
                '}';
    }

    @Override
    public int compareTo(Object o) {
        Pair p = (Pair) o ;
        if (this.value instanceof Integer){
            Integer x = (int)this.getValue() ;
            Integer y = (int)p.getValue();
            return x.compareTo(y) ;
        }
        else if (this.value instanceof Double){
            Double x = (double)this.getValue() ;
            Double y = (double)p.getValue();
            return x.compareTo(y) ;
        }
        else if (this.value instanceof String){
            String x = (String)this.getValue() ;
            String y = (String) p.getValue();
            return x.compareTo(y) ;

        }
        else if (this.value instanceof Date){
            Date x = (Date)this.getValue() ;
            Date y = (Date)p.getValue();
            return x.compareTo(y) ;
        }
        else {
            System.out.println("Something went wrong in comparing");
            return 0 ;
        }


    } // end of compareTo
}  // end of class
