import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;

public class test {
    public static void main(String[] args) throws IOException, DBAppException, ParseException {
//        DBApp x=new DBApp();
//        x.init();
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
        //x.createTable( "table1", "id", htblColNameType,htblColNameMin,htblColNameMax );
//        DBTable table = x.getDb().get((String)"table1");
//        Vector pages = table.getPages();
//        System.out.println(pages);
//        System.out.println(x.getDb().get((String)"table1").getPages());
//
//
//
//
//        Hashtable h1 = new Hashtable( );
//        h1.put("id", new Integer(5));
//        h1.put("name", new String("ale2" ) );
//        h1.put("gpa", new Double( 0.95 ) );
//        Hashtable h2 = new Hashtable( );
//        h2.put("id", new Integer(15));
//        h2.put("name", new String("afdgsgsag" ) );
//        h2.put("gpa", new Double( 5 ) );
//        Hashtable h3 = new Hashtable( );
//        h3.put("id", new Integer(25));
//        h3.put("name", new String("afdghfghjfgjfsgsag" ) );
//        h3.put("gpa", new Double( 99 ) );
//        Hashtable h4 = new Hashtable( );
//        h4.put("id", new Integer(35));
//        h4.put("name", new String("lol" ) );
//        h4.put("gpa", new Double( 6 ) );
//        Hashtable h5 = new Hashtable( );
//        h5.put("id", new Integer(45));
//        h5.put("name", new String("five" ) );
//        h5.put("gpa", new Double( 6 ) );
//        Hashtable h6 = new Hashtable( );
//        h6.put("id", new Integer(55));
//        h6.put("name", new String("six" ) );
//        h6.put("gpa", new Double( 6 ) );
//        Hashtable h7 = new Hashtable( );
//        h7.put("id", new Integer(65));
//        h7.put("name", new String("seven" ) );
//        h7.put("gpa", new Double( 6 ) );
//
//        Hashtable h8 = new Hashtable( );
//        h8.put("id", new Integer(11));
//        h8.put("name", new String("eight" ) );
//        h8.put("gpa", new Double( 6 ) );
//        Hashtable h9 = new Hashtable( );
//        h9.put("id", new Integer(30));
//        h9.put("name", new String("nine" ) );
//        h9.put("gpa", new Double( 6 ) );
//
//
//
//
//        x.insertIntoTable("table1",h1);

////        System.out.println("First Insertion");
////        System.out.println(p.getTuples());
////        System.out.println("Max is   "+p.getMax());
////        System.out.println("Min is   "+p.getMin());
////        System.out.println("Current number of pages =   "+table.getPages().size());
//
////        x.insertIntoTable("table2",h2);
////        curPage= ((Vector)table.getPages().get(0));
////        p = x.deSerializePage((String)curPage.get(0));
////        System.out.println("Second Insertion");
////        System.out.println(p.getTuples());
////        System.out.println("Max is   "+p.getMax());
////        System.out.println("Min is   "+p.getMin());
////        System.out.println("Current number of pages =   "+table.getPages().size());
//
//
////        x.insertIntoTable("table2",h3);
////        curPage= ((Vector)table.getPages().get(0));
////        p = x.deSerializePage((String)curPage.get(0));
////        System.out.println("Third Insertion");
////        System.out.println(p.getTuples());
////        System.out.println("Max is   "+p.getMax());
////        System.out.println("Min is   "+p.getMin());
////        System.out.println("Current number of pages =   "+table.getPages().size());
//
//
////        x.insertIntoTable("table2",h4);
////        curPage= ((Vector)table.getPages().get(1));
////        p = x.deSerializePage((String)curPage.get(0));
////        System.out.println("Forth Insertion");
////        System.out.println(p.getTuples());
////        System.out.println("Max is   "+p.getMax());
////        System.out.println("Min is   "+p.getMin());
////        System.out.println("Current number of pages =   "+table.getPages().size());
//
//
//
////        x.insertIntoTable("table2",h5);
////        x.insertIntoTable("table2",h6);
////        x.insertIntoTable("table2",h7);
////        x.insertIntoTable("table2",h8);
////        x.insertIntoTable("table2",h9);
//
//
//
//        Vector curPage= ((Vector)table.getPages().get(0));
//        Page p = null;
//
//        p = x.deSerializePage((String)curPage.get(0));
//
//        System.out.println(p.getTuples());
////        System.out.println(((Record)p.getTuples().get(0)).getData().get(0).getValue().getClass());
////        System.out.println(((Record)p.getTuples().get(0)).getData().get(1).getValue().getClass());
////        System.out.println(((Record)p.getTuples().get(0)).getData().get(2).getValue().getClass());
////
//        Hashtable h00 = new Hashtable( );
////        h8.put("id", new Integer(11));
//        h00.put("name", new String("zero" ) );
//        h00.put("gpa", new Double( 3 ) );
//        x.updateTable("table2","11",h00);

//        p = x.deSerializePage((String)curPage.get(0));
//
//        System.out.println(p.getTuples());





//
//        Record r1 = new Record(h1,"id");
//        Record r2 = new Record(h2,"id");
//        Record r3 = new Record(h3,"id");
//        Record r4 = new Record(h4,"id");
//        Record r5 = new Record(h5,"id");
//        Record r6 = new Record(h6,"id");
//        Record r7 = new Record(h7,"id");
//        Record r8 = new Record(h8,"id");
//
//        Vector v1 = createVector(r1);
//        Vector v2 = createVector(r2);
//        Vector v3 = createVector(r3);
//        Vector v4 = createVector(r4);
//        Vector v5 = createVector(r5);
//        Vector v6 = createVector(r6);
//        Vector v7 = createVector(r7);
//
//        pages.add(v1);
//        pages.add(v2);
//        pages.add(v3);
//        pages.add(v4);
//        pages.add(v5);
//        pages.add(v6);
//        pages.add(v7);
//        System.out.println(pages);
//        table.setPages(pages);
//        System.out.println( table.searchPage(r8));






//        Page p = new Page("CityShop");
//        p.insertRecord(r1);
//        p.insertRecord(r2);
//        p.insertRecord(r3);

//        p.insertRecord(r5);
//        p.insertRecord(r6);
//        p.insertRecord(r7);
//        x.insertIntoTable("test",h1);
//        //x.insertIntoTable("test",h1);
//        Page p=x.deSerializePage("test0");
//        System.out.println(p.getTuples());
//        Comparator<Record> c = new Comparator<Record>() {
//            public int compare(Record u1, Record u2)
//            {
//                return ((Pair)u1.getData().get(0)).compareTo((Pair)u2.getData().get(0));
//            }};
//        System.out.println(Collections.binarySearch(p.getTuples(),r4,c));
//        System.out.println( p.searchRecord(r4));
//        p.insertRecord(r4);
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







    }//end of main


    public static Vector createVector (Record r ){
        Vector v = new Vector();
        v.add("Somewhere");
        Integer m = (Integer)r.getData().get(0).getValue() ;
        v.add(m+9);
        v.add(m);


        return  v ;
    }






}
