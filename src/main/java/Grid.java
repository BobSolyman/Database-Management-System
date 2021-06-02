import java.io.Serializable;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class Grid implements Serializable {
    // V,X
    private Vector<String> columns;
    //key -> values--> <v1,X2>-->bucket
    private Hashtable<Vector, String> buckets;
    private Hashtable <String , Vector<Object>> cellRange ;
    //[X,Y,Z...]
    private Hashtable<String,Object> range;
    private Hashtable<String,Object> min;
    private Hashtable<String,Object> max;
    private Hashtable<String,String> type;
    private Hashtable<Character,Integer> charToInt ;
    private Hashtable<Integer, Character> intToChar ;
    private String gridID ;

    //multiple variations can point to the same bucket!

    public Grid(String[] columns, Hashtable<String, Object> mi, Hashtable<String, Object> ma, Hashtable<String, String> t) {
        this.columns = new Vector<>();
        this.columns.addAll(Arrays.asList(columns));
        Collections.sort(this.columns);
        this.buckets = new Hashtable();
        this.range = new Hashtable();
        this.min = mi;
        this.max = ma;
        this.type = t;
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        gridID = (new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(timestamp)).toString() ;

        for (String col : this.columns){
            if (((String)type.get(col)).equals("java.lang.Integer")){
                int step = (Integer.parseInt((String)max.get(col))-Integer.parseInt((String)min.get(col)))/9;
                range.put(col,step);
            }
            else if (((String)type.get(col)).equals("java.lang.Double")){
                double step = (Double.parseDouble((String)max.get(col))-Double.parseDouble((String)min.get(col)))/9;
                range.put(col,step);
            }
            else if (((String)type.get(col)).equals("java.util.Date")){
                Date minDate = null;
                Date maxDate = null;
                try {
                    minDate = new SimpleDateFormat("yyyy-MM-dd").parse((String)min.get(col));
                    maxDate =  new SimpleDateFormat("yyyy-MM-dd").parse((String)max.get(col));
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                int step = (maxDate.getDate()-minDate.getDate())/9;
                range.put(col,step);



            }else if (((String)type.get(col)).equals("java.lang.String")){
                char a = 'a';
                charToInt = new Hashtable<>();
                intToChar = new Hashtable<>();
                for (int o = 1 ; o <27 ; o++){
                    charToInt.put(a,o);
                    intToChar.put(o,a++);
                }


                if (((String) min.get(col)).charAt(0)<='9' && ((String) min.get(col)).charAt(0)>='0'){ // id case
                    String[] maxPayne = ((String) max.get(col)).split("-");
                    String[] minPayne = ((String) min.get(col)).split("-");
                    int maxWell = Integer.parseInt(maxPayne[0]+maxPayne[1]);
                    int minWell = Integer.parseInt(minPayne[0]+minPayne[1]);
                    long step = (maxWell - minWell)/9;
                    range.put(col,step);
                }
                else {
                long maxValue = 0 ;
                int weight = 0 ;
                for (int i = ((String) max.get(col)).length()-1 ; i>=0 ; i--){
                    char x = ((String) max.get(col)).toLowerCase().charAt(i);
                    maxValue = maxValue + ((26^weight++)*(int)charToInt.get(x));
                }
                long minValue = 0 ;
                weight = 0 ;
                for (int i = ((String) min.get(col)).length()-1 ; i>=0 ; i--){
                    char x = ((String) min.get(col)).toLowerCase().charAt(i);
                    minValue = minValue + ((26^weight++)*(int)charToInt.get(x));
                }
                long step = (maxValue - minValue)/9;
                range.put(col,step);
                }// end if it was a string

                // trial to use permutations to solve string ranges
//                long noComb = 1;
//                long res = 0 ;
//                int maxlen = ((String) max.get(col)).length();
//                int minlen = ((String) min.get(col)).length();
//                boolean flag = true ;
//                for(int j = minlen ; j <= maxlen; j++) {
//                    for (int i = 0; i < j; i++) {
//                        flag = true;
//                        if (i < ((String) min.get(col)).length()) {
//                            int diff = (int) ((String) max.get(col)).charAt(i) - (int) ((String) min.get(col)).charAt(i);
//                            if (((String) min.get(col)).charAt(i)>='0' && ((String) min.get(col)).charAt(i)<='9'){
//                                flag = false;
//                            }
//                            if (diff < 0 ) {
//                                if (flag) {
//                                    diff = diff + 26;
//                                }
//                                else {
//                                    diff = diff + 10 ;
//                                }
//                            }
//                            else if (diff == 0) {
//                                diff = 1;
//                            }
//                            noComb = noComb * diff;
//                        } else {
//                            if (flag) {
//                                noComb = noComb * 26;
//                            }
//                            else {
//                                noComb = noComb * 10;
//                            }
//                        }
//                    }
//                    res = noComb + res ;
//                    noComb = 1 ;
//                }
//                res = res/9;
//                range.put(col,res);
            }


        }

    }// END OF CONSTRUCTOR




    public Vector<Integer> getIndex (Record r){
        Vector <Integer> res = new Vector<>();

        for (String col : this.columns){

            if (((String)type.get(col)).equals("java.lang.Integer")){
                int step = (int)range.get(col);
                int i = 0 ;
                if(r.getContent().containsKey(col))
                     i = ((int)r.getContent().get(col)-Integer.parseInt((String)min.get(col)))/step;
                else
                     i = -1 ;
                res.add(i);
            }
            else if (((String)type.get(col)).equals("java.lang.Double")){
                double step = (double)range.get(col);
                double i = 0 ;
                if(r.getContent().containsKey(col))
                    i = ((double)r.getContent().get(col)-Double.parseDouble((String)min.get(col)))/step;
                else
                    i = -1 ;
                res.add((int)i);

            }
            else if (((String)type.get(col)).equals("java.util.Date")){
                int step = (int)range.get(col);
                int i = 0 ;
                if(r.getContent().containsKey(col)){
                    Date minDate = null;
                    Date maxDate = null;
                    try {
                        minDate = new SimpleDateFormat("yyyy-MM-dd").parse((String)min.get(col));
                        maxDate =  (Date)r.getContent().get(col);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    i = (maxDate.getDate()-minDate.getDate())/step;
                }

                else
                    i = -1;
                res.add(i);

            }else if (((String)type.get(col)).equals("java.lang.String")){
                int i = 0;
                long step = (long)range.get(col);

                if(r.getContent().containsKey(col)){
                    if (((String)r.getContent().get(col)).charAt(0)<='9' && ((String)r.getContent().get(col)).charAt(0)>='0'){ // id case
                        String[] maxPayne = ((String)r.getContent().get(col)).split("-");
                        String[] minPayne = ((String) min.get(col)).split("-");
                        int maxWell = Integer.parseInt(maxPayne[0]+maxPayne[1]);
                        int minWell = Integer.parseInt(minPayne[0]+minPayne[1]);
                        i = (int)((maxWell - minWell)/step);
                    }
                    else{
                        long maxValue = 0 ;
                        int weight = 0 ;
                        for (int j = ((String)r.getContent().get(col)).length()-1 ; j>=0 ; j--){
                            char x = ((String)r.getContent().get(col)).toLowerCase().charAt(j);
                            maxValue = maxValue + ((26^weight++)*(int)charToInt.get(x));
                        }
                        long minValue = 0 ;
                        weight = 0 ;
                        for (int j = ((String) min.get(col)).length()-1 ; j>=0 ; j--){
                            char x = ((String) min.get(col)).toLowerCase().charAt(j);
                            minValue = minValue + ((26^weight++)*(int)charToInt.get(x));
                        }
                        i = (int)((maxValue - minValue)/step);
                    }

                }
                else
                    i = -1;
                res.add(i);
                // case of permutation trial
//                if(r.getContent().containsKey(col)) {
//                    String s = (String) r.getContent().get(col);
//                    long noComb = 1;
//                    long result = 0 ;
//                    boolean flag = true ;
//                    int maxlen = s.length();
//                    int minlen = ((String) min.get(col)).length();
//                    for(int j = minlen ; j <= maxlen; j++) {
//                        for (int k = 0; k < j; k++) {
//                            flag=true;
//                            if (k < ((String) min.get(col)).length()) {
//                                int diff = (int) s.charAt(k) - (int) ((String) min.get(col)).charAt(k);
//                                if (s.charAt(k)>='0' && s.charAt(k)<='9'){
//                                    flag = false;
//                                }
//                                if (diff < 0 ) {
//                                    if (flag) {
//                                        diff = diff + 26;
//                                    }
//                                    else {
//                                        diff = diff + 10;
//                                    }
//                                }
//                                else if (diff == 0) {
//                                    diff = 1;
//                                }
//
//                                noComb = noComb * diff;
//                            } else {
//                                if (flag) {
//                                    noComb = noComb * 26;
//                                }
//                                else {
//                                    noComb = noComb * 10;
//                                }
//                            }
//                        }
//                        result = noComb + result ;
//                        noComb = 1 ;
//                    }
//                    long step = (long)range.get(col);
//                    i =(int)(result / step) ;
//
//                }
//                else {
//                    i = -1 ;
//                }
//                res.add(i);
            }
        } // end of iterating over the columns

     return res ;
    }

    public String getGridID() {
        return gridID;
    }

    public Vector<String> getColumns() {
        return columns;
    }

    public void setColumns(Vector<String> columns) {
        this.columns = columns;
    }

    public Hashtable<Vector, String> getBuckets() {
        return buckets;
    }

    public void setBuckets(Hashtable<Vector, String> buckets)
    {
        this.buckets = buckets;
    }

    public Hashtable<String, Object> getRange() {
        return range;
    }

    public void setRange(Hashtable<String, Object> range) {
        this.range = range;
    }

    public Hashtable<String, Object> getMin() {
        return min;
    }

    public void setMin(Hashtable<String, Object> min) {
        this.min = min;
    }

    public Hashtable<String, Object> getMax() {
        return max;
    }

    public void setMax(Hashtable<String, Object> max) {
        this.max = max;
    }

    public Hashtable<String, String> getType() {
        return type;
    }

    public void setType(Hashtable<String, String> type) {
        this.type = type;
    }
}

//whenever we create a key we create a bucket with the range of values
