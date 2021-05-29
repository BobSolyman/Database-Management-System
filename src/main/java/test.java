import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class test {
    public static void main(String[] args) throws IOException, DBAppException, ParseException {
        DBApp x=new DBApp();
        x.init();
        Hashtable htblColNameType = new Hashtable( );
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");
        Hashtable htblColNameMin = new Hashtable( );
        htblColNameMin.put("id", "0");
        htblColNameMin.put("name","a");
        htblColNameMin.put("gpa", "0");
        Hashtable htblColNameMax = new Hashtable( );
        htblColNameMax.put("id", "999");
        htblColNameMax.put("name", "zzzzzzzzz");
        htblColNameMax.put("gpa", "999.999");
//        x.createTable( "test", "id", htblColNameType,htblColNameMin,htblColNameMax );
//        DBTable table = x.getDb().get((String)"test");
//        Vector pages = table.getPages();
//        System.out.println(pages);
//        System.out.println(x.getDb().get((String)"testDate").getPages().size());
//        Date p =new SimpleDateFormat("yyyy-MM-dd").parse((String)x.getDb().get((String)"testDate").getColNameMin().get("id"));
//        System.out.println(p);
//        System.out.println((new Date(2000-1900,8-1,9)));
//

//
// line 461 in update table
//

//
//
//        Hashtable h1 = new Hashtable( );
//        h1.put("id", new Date(2000,8,9));
//        h1.put("name", new String("ale2" ) );
//        h1.put("gpa", new Double( 0.95 ) );
//
//        x.insertIntoTable("testDate",h1);
//        System.out.println(x.getDb().get((String)"testDate").getPages().size());
//
//
//        Hashtable h2 = new Hashtable( );
//        h2.put("id", new Date(2010,4,7));
//        h2.put("name", new String("afdgsgsag" ) );
//        h2.put("gpa", new Double( 5 ) );
//        Hashtable h3 = new Hashtable( );
//        h3.put("id", new Date(1950,4,9));
//        h3.put("name", new String("afdghfghjfgjfsgsag" ) );
//        h3.put("gpa", new Double( 99 ) );
//
//        x.insertIntoTable("testDate",h2);
//        System.out.println(x.getDb().get((String)"testDate").getPages().size());
//        x.insertIntoTable("testDate",h3);
//        System.out.println(x.getDb().get((String)"testDate").getPages().size());




//----------------
        Hashtable h41 = new Hashtable( );
        h41.put("id", new Integer(1));
//        h41.put("name",new String("first") );
//        h41.put("gpa", new Double( 6 ) );


        Hashtable h42 = new Hashtable( );
        h42.put("id", new Integer(3));
//        h42.put("name",new String("second"));
////        h42.put("gpa", new Double( 6 ) );


        Hashtable h43 = new Hashtable( );
        h43.put("id", new Integer(5));
//        h43.put("name",new String("7aga") );
////        h43.put("gpa", new Double( 4 ) );



        Hashtable h44 = new Hashtable( );
        h44.put("id", new Integer(10));
//        h44.put("name",new Date (2012-1900,12-1,12) );
//        h44.put("gpa", new Double( 6 ) );


        Hashtable h45 = new Hashtable( );
        h45.put("id", new Integer(11));
//        h45.put("name",new Date (2012-1900,12-1,12) );
//        h45.put("gpa", new Double( 1 ) );



        Hashtable h46 = new Hashtable( );
        h46.put("id", new Integer(12));
//        h46.put("name",new Date (2012-1900,12-1,12) );


        Hashtable h47 = new Hashtable( );
        h47.put("id", new Integer(50));
//        h47.put("name",new Date (2002-1900,4-1,13) );
//        h47.put("gpa", new Double( 3.2 ) );

//
//        x.insertIntoTable("test",h41);
//        x.insertIntoTable("test",h42);
//        x.insertIntoTable("test",h43);
//        x.insertIntoTable("test",h44);
//        x.insertIntoTable("test",h45);
//        x.insertIntoTable("test",h46);
//        x.insertIntoTable("test",h47);
//        x.deleteFromTable("test",h43);


//        table = x.getDb().get((String)"test");

//        Vector curPage= ((Vector)table.getPages().get(0));
//        Page p = null;
//        System.out.println(table.getPageID());
//        for (int i=0; i<table.getPages().size(); i++){
//            curPage= ((Vector)table.getPages().get(i));
////            System.out.println(curPage.get(0));
//            p = x.deSerializePage((String)curPage.get(0));
//            System.out.println(p.getTuples());
//        }
//        String a = "aaa";
//
//        byte [] aa = a.getBytes(StandardCharsets.UTF_8);
//        String z = "zzzz";
//        byte [] zz = z.getBytes(StandardCharsets.UTF_8);
//        byte [] res = new byte [aa.length];
//        int jump = (int)zz[0]-(int)aa[0];
//
//
//            int r = (10-1)/9 ;
//        System.out.println(r);


//        for (int j = 0 ;j <12 ;j++){
//            for ( int i = 0 ; i < aa.length;i++){
//                res[i]=(byte)(aa[i]+(((int)zz[i] - (int)aa[i])/10*j));
//
//            }
//            System.out.println(new String(res));
//        }



        //        Hashtable h48 = new Hashtable( );
//        h48.put("id", new Integer(21));
//        h48.put("name",new Date (2012-1900,12-1,12) );
//        h48.put("gpa", new Double( 1.68 ) );
//
//        x.insertIntoTable("testdelete",h48);
//
//        Hashtable h49 = new Hashtable( );
//        h49.put("id", new Integer(22));
//        h49.put("name",new Date (2000-1900,9-1,7) );
//        h49.put("gpa", new Double( 1.92 ) );
//
//        x.insertIntoTable("testdelete",h49);
        //=============
//

//
//        p = x.deSerializePage((String)curPage.get(0));
//        System.out.println(p.getTuples());
//        curPage= ((Vector)table.getPages().get(1));
//        p = x.deSerializePage((String)curPage.get(0));
//        System.out.println(p.getTuples());
//        curPage= ((Vector)table.getPages().get(2));
//        p = x.deSerializePage((String)curPage.get(0));
//        System.out.println(p.getTuples());
//        System.out.println("---------------------------");
//
//        Hashtable h466 = new Hashtable( );
//        h466.put("name",new Date (2012-1900,12-1,12) );
//
//        x.deleteFromTable("testdelete", h466);
//        p = x.deSerializePage((String)curPage.get(0));
//        System.out.println(p.getTuples());
//        curPage= ((Vector)table.getPages().get(1));
//        p = x.deSerializePage((String)curPage.get(0));
//        System.out.println(p.getTuples());

//        Hashtable h5 = new Hashtable( );
////        h5.put("id", new Integer(45));
//        h5.put("name","legenDARY!!");


//        x.updateTable("testDate","1950-04-09",h5);
//
//        curPage= ((Vector)table.getPages().get(0));
//        p = null;
//
//        p = x.deSerializePage((String)curPage.get(0));
//
//        System.out.println(p.getTuples());


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

//     Page p = new Page("lol");
//        Hashtable h6 = new Hashtable( );
//        h6.put("id", new Integer(55));
//        h6.put("name", new String("six" ) );
//        h6.put("gpa", new Double( 6 ) );
//     Record r = new Record(h6,"id");
//     p.insertRecord(r);
//     x.serializePage(p,"lol");
//     bucketEntry bE = new bucketEntry(r,"lol");
//     x.serialize(bE,"bElol");
//        System.out.println(bE.getRow());







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
