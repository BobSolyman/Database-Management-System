import java.beans.Transient;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class Page implements Serializable {
    private int noRows;
    private Vector <Record>tuples;
    private String table;
    private Object  min;
    private Object  max;
    private static int maxPage;

    public Page(String table) throws IOException {
        this.table=table;
        this.maxPage= readingFromConfigFile("MaximumRowsCountinPage");
        tuples = new Vector<>();
    }


    public void insertRecord (Record r) throws DBAppException { // this inserts in the right place
        if (this.tuples.size()==0)
            this.tuples.add(r);
       else {
           int i = 0 ;
           int indexF = 0 ;
           Record f = this.tuples.get(indexF);
           int indexL =this.tuples.size()-1;
           Record l = this.tuples.get(indexL);
           int indexM = indexF + (indexL - indexF)/2 ;
           Record m = this.tuples.get(indexM);

           if (f.compareTo(r)>0){  // first record greater than input
               this.tuples.add(0,r);
           }
           else if (r.compareTo(l)>0){
               this.tuples.add(this.tuples.size(),r);
           }
           else if (r.compareTo(f)>0 && r.compareTo(l)<0){  // input is in this page for sure
               while (indexF<=indexL){
                   indexM = indexF + (indexL - indexF)/2 ;
                   m = this.tuples.get(indexM);
                   f = this.tuples.get(indexF);
                   l = this.tuples.get(indexL);

                   if (r.compareTo(m)>=0){ // meaning upper half
                      indexF = indexM + 1;

                   }
                   else {                   //meaning lower half
                       indexL = indexM - 1 ;
                   }
               }//end of our loop
               i = indexF ;
//               System.out.println(i);
               this.tuples.add(i,r);

           }
           else {
               throw new DBAppException("Tuple Already Exists");
           }



        }//end of else

    }//end of method


    public int [] searchRecord (Record r){
        int [] target = new int [2];
        int i = 0 ;
        Comparator<Record> c = new Comparator<Record>() {
            public int compare(Record u1, Record u2)
            {
                return ((Pair)u1.getData().get(0)).compareTo((Pair)u2.getData().get(0));
            }};

        i = Collections.binarySearch(this.getTuples(),r,c);
        if (i >= 0){
           target[0] = i;
           target[1] = 1 ;
        }
        else {
            i = Math.abs(i +1);
            target[0] = i;
            target[1] = 0 ;
        }
        return target ;

    }

    public void deleteRecord(Record r){
        for (int i=this.getTuples().size()-1; i>=0; i--){
            Record currentRec = (Record)this.getTuples().get(i);
            boolean mismatch = false;
            for (int j=1; j<currentRec.getData().size(); j++){
                for (int k=0; k<r.getData().size(); k++){
                    if(currentRec.getData().get(j).getKey().equals(r.getData().get(k).getKey())){
                        if (!currentRec.getData().get(j).getValue().equals(r.getData().get(k).getValue())) {
                            mismatch = true;
                            break;
                        }
                    }
                }
                if (mismatch){
                    break;
                }
            }
            if(!mismatch){
                this.getTuples().remove(i);
            }
        }
    }


    public Object getMin() {
        return min;
    }

    public void setMin(Object min) {
        this.min =  min;
    }

    public Object getMax() {
        return max;
    }

    public void setMax(Object max) {
        this.max =  max;
    }


    public void setNoRows(int noRows) {
        this.noRows = noRows;
    }


    public  int getMaxPage() { return maxPage; }

    public Vector getTuples() {
        return tuples;
    }


    public String getTable() { return table; }

    public static int readingFromConfigFile(String string) throws IOException {
        Properties prop = new Properties();
        FileInputStream property = new FileInputStream("src/main/resources/DBApp.config");
        prop.load(property);
        return Integer.parseInt(prop.getProperty(string));
    }
}
