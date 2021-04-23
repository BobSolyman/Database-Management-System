import java.io.Serializable;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;

public class Record implements Comparable, Serializable {

     private Vector <Pair> data ;

    public Vector<Pair> getData() {
        return data;
    }

    public void setData(Vector<Pair> data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "{"+ data +
                '}';
    }

    public Record(Hashtable<String, Object> h , String cKey) {
        Pair Primary = null ;
        this.data = new Vector<>();
        for(Map.Entry m: h.entrySet()){
          Pair p =  new Pair( m.getKey().toString() , m.getValue()) ;
          if (m.getKey().toString().equals(cKey))
              Primary = p ;
          else
              data.add(p);
        }
        data.add(0,Primary);

    }

    @Override
    public int compareTo(Object o) {
        Record r = (Record) o ;
        return this.getData().get(0).compareTo(r.getData().get(0));
    }

}
