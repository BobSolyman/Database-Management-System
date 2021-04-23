import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Vector;

public class test {
    public static void main(String[] args) throws IOException, DBAppException {
        DBApp x=new DBApp();
        x.init();
//        Hashtable htblColNameType = new Hashtable( );
//        htblColNameType.put("id", "java.lang.Integer");
//        htblColNameType.put("name", "java.lang.String");
//        htblColNameType.put("gpa", "java.lang.Double");
//        Hashtable htblColNameMin = new Hashtable( );
//        htblColNameMin.put("id", "0");
//        htblColNameMin.put("name", "a");
//        htblColNameMin.put("gpa", "0");
//        Hashtable htblColNameMax = new Hashtable( );
//        htblColNameMax.put("id", "999");
//        htblColNameMax.put("name", "ZZZ");
//        htblColNameMax.put("gpa", "999.999");
//        x.createTable( "test", "id", htblColNameType,htblColNameMin,htblColNameMax );

        Hashtable h1 = new Hashtable( );
        h1.put("id", new Integer( 2343432 ));
        h1.put("name", new String("Ahmed Noor" ) );
        h1.put("gpa", new Double( 0.95 ) );
        Hashtable h2 = new Hashtable( );
        h2.put("id", new Integer( 88 ));
        h2.put("name", new String("afdgsgsag" ) );
        h2.put("gpa", new Double( 5 ) );
        Hashtable h3 = new Hashtable( );
        h3.put("id", new Integer( 1 ));
        h3.put("name", new String("afdghfghjfgjfsgsag" ) );
        h3.put("gpa", new Double( 99 ) );

        Vector <Hashtable> tuples = new Vector<>();
        tuples.add(h1);
        tuples.add(h2);
        tuples.add(h3);
        
        System.out.println(tuples.toString());





    }
}
