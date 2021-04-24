import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Vector;

public class test {
    public static void main(String[] args) throws IOException, DBAppException, ParseException {
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
        h1.put("id", "5");
        h1.put("name", new String("Ahmed Noor" ) );
        h1.put("gpa", new Double( 0.95 ) );
//        Hashtable h2 = new Hashtable( );
//        h2.put("id", "mahmoud");
//        h2.put("name", new String("afdgsgsag" ) );
//        h2.put("gpa", new Double( 5 ) );
//        Hashtable h3 = new Hashtable( );
//        h3.put("id", "shehab");
//        h3.put("name", new String("afdghfghjfgjfsgsag" ) );
//        h3.put("gpa", new Double( 99 ) );
//        Hashtable h4 = new Hashtable( );
//        h4.put("id", "peter");
//        Hashtable h5 = new Hashtable( );
//        h5.put("id", "farida");
//        Hashtable h6 = new Hashtable( );
//        h6.put("id", "zamalek");
//        Hashtable h7 = new Hashtable( );
//        h7.put("id", "beck");



        Record r1 = new Record(h1,"id");
//        Record r2 = new Record(h2,"id");
//        Record r3 = new Record(h3,"id");
//        Record r4 = new Record(h4,"id");
//        Record r5 = new Record(h5,"id");
//        Record r6 = new Record(h6,"id");
//        Record r7 = new Record(h7,"id");

//        Page p = new Page("CityShop");
//        p.insertRecord(r1);
//        p.insertRecord(r2);
//        p.insertRecord(r3);
//        p.insertRecord(r4);
//        p.insertRecord(r5);
//        p.insertRecord(r6);
//        p.insertRecord(r7);
        x.insertIntoTable("test",h1);
        x.insertIntoTable("test",h1);
        //Page p=x.deSerializePage("test0");
        System.out.println("ba".compareTo("bm"));
//
//        Vector a= new Vector();
//        Vector b= new Vector();
//        a.add("hi");
//        a.add("22");
//        b.add("hi");
//        b.add("22");
//        Vector c= new Vector();
//        c.add(a);
       // System.out.println("B".compareTo("A"));







    }
}
