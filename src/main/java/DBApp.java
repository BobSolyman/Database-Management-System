import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class DBApp implements DBAppInterface{

     private HashMap<String,DBTable> db= new HashMap();
     private int maxTuples;

    {
        try {
            maxTuples = Page.readingFromConfigFile("MaximumRowsCountinPage");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public HashMap<String, DBTable> getDb() {
        return db;
    }

    //We init a DB by giving ColNameType hashtable ex put.("ID",INT) ; put.("Name",String);
    //Max and Min are handled by giving type +MAX/MIN ex Min.put("ID", "0"); Max.put("ID","1");
    //key is name
    //value is type

    //Starting Logic:
    //We will read the prev hashmap, and set it = to the global db
    //db will only store DBTable and the name inside it
    //actual tuples will be stored on pages and on the DBTable dataType

    public void init() {
        //check if row config exists
        //check if Db exists
        //we need to translate metadata into map (review)
        HashMap<String,DBTable>  map;
        try {
            if(getFileSize("src/main/resources/metadata.csv")>0){
            db = getMap("src/main/resources/metadata.csv");
//            db.get("CityShop").displayAttributes();
            //db.get("test").displayAttributes();
            } else {
                FileWriter fr = new FileWriter("src/main/resources/metadata.csv");
                BufferedWriter br2 = new BufferedWriter(fr);
                String d = "Table Name, Column Name, Column Type, ClusteringKey, Indexed , min , max"+"\n";
                br2.write(d);
                br2.close();
                fr.close();
//               System.out.println("gowa el else");
            }

                for (String table : db.keySet()) {
                    readLocation(table);
                }


        } catch (IOException i) {
            i.printStackTrace();
            System.out.println("NO PREVIOUS DB!!!");
            return;
        }



    }

    public static int getFileSize(String file) throws IOException {
       int i= 0;
        try{
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            while (br.readLine() != null){
                i++;
            }
            br.close();
            fr.close();
        }
        catch(IOException e){

            return 0;
        }
        return i;
    }
    //Method to read a file
    public static HashMap<String,DBTable> getMap(String filePath)throws IOException{
        HashMap<String,DBTable> temp = new HashMap();
        FileReader fr = new FileReader(filePath);
        BufferedReader br = new BufferedReader(fr);
        String CurrentLine = "";
        String cKey = null;

        CurrentLine=br.readLine();
        while((CurrentLine=br.readLine())!=null){
            String[] line = CurrentLine.split(",");
            String tableName = line[0].trim();
            String colName = line[1].trim();
            String colType = line[2].trim();
            String clusterKey = line[3].trim();

            if(clusterKey.toLowerCase().equals("true")){
                cKey = colName;
            }
            String indexed = line[4].trim();
            String min = line[5].trim();
            String max = line[6].trim();

            if(temp.containsKey(tableName)){
                temp.get(tableName).addColumn(colName,colType,min,max);
                if (cKey!=null) {
                    temp.get(tableName).setClusteringKey(cKey);
                    cKey = null;
                }
            }
            else{
                Hashtable<String,String> newColType = new Hashtable<>();
                newColType.put(colName,colType);
                Hashtable<String,String> newColMin = new Hashtable<>();
                newColMin.put(colName,min);
                Hashtable<String,String> newColMax = new Hashtable<>();
                newColMax.put(colName,max);
                DBTable newTable = new DBTable(tableName,cKey,newColType);
                if (cKey!=null)
                    cKey=null;
                newTable.setColNameMin(newColMin);
                newTable.setColNameMax(newColMax);
                temp.put(tableName,newTable);
            }
        }
        return temp;
    }


    public boolean checkCSV (String filePath, String tableName, Hashtable<String, String> colNameType ){
      boolean flag = true ;
        try {
            FileReader fr = new FileReader(filePath);
            BufferedReader br = new BufferedReader(fr);
            String CurrentLine = "";

            CurrentLine = br.readLine();
            while ((CurrentLine = br.readLine()) != null) {
                String[] line = CurrentLine.split(",");
                String tName = line[0].trim();
                String colName = line[1].trim();
                if (tName.equals(tableName) && colNameType.containsKey(colName)){
                    return false ;
                    }

                }
                }

        catch(Exception e){
            e.printStackTrace();
        }
    return true ;
    }





    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {

        validate(tableName,clusteringKey,colNameType,colNameMin,colNameMax);

        try{
            FileWriter fr = new FileWriter("src/main/resources/metadata.csv",true);
            BufferedWriter br2 = new BufferedWriter(fr);


            if (checkCSV("src/main/resources/metadata.csv",tableName,colNameType)) {
                DBTable current = new DBTable(tableName, clusteringKey, colNameType);
                current.setColNameMin(colNameMin);
                current.setColNameMax(colNameMax);
                db.put(tableName,current);

                Set <String> keys =colNameType.keySet();
                for(String key: keys){
                    String type = colNameType.get(key);
                    String min = colNameMin.get(key);
                    String max = colNameMax.get(key);
                    if (key.equals(clusteringKey)){    // index default is false
                        br2.write(tableName+","+key+","+type+","+"TRUE"+","+"FALSE"+","+min+","+max+"\n");
                    }
                    else {
                        br2.write(tableName+","+key+","+type+","+"FALSE"+","+"FALSE"+","+min+","+max+"\n");
                    }

                }

            }
            else {
                throw new DBAppException("ATTRIBUTE MUST BE UNIQUE!!!") ;

            }
            br2.close();
            fr.close();

        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public void createIndex(String tableName, String[] columnNames) throws DBAppException {
        //Milestone 2
    }

    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException, ParseException {

        validate(tableName,colNameValue);

        //Get the table and find the clustering key
        DBTable curTable= db.get(tableName);
        Object clusteringKey= curTable.getClusteringKey();
        Object curKey= colNameValue.get(clusteringKey);


        //Check if there are no pages in the table
        if(curTable.getPages().size() == 0){
            try {
                Page p = new Page(tableName);
                Record r = new Record(colNameValue,(String) clusteringKey);
                p.getTuples().add(r);
                p.setMax(r.getData().get(0).getValue());
                p.setMin(r.getData().get(0).getValue());
                p.setNoRows(p.getTuples().size());
                serializePage(p,tableName+db.get(tableName).getPageID());
                updateLocation(tableName+db.get(tableName).getPageID(),p,0,false);
                db.get(tableName).setPageID(db.get(tableName).getPageID()+1);
                //System.out.println(curTable.getPages());
            }
            catch(IOException e){
                e.printStackTrace();
            }
        }
//
        else {
            Record r = new Record(colNameValue,(String) clusteringKey);
            int indexP = curTable.searchPage(r);
//            System.out.println("IndexP is  " +indexP);
            boolean flag = false ;
            Page p = null ;
            Record shifter = null ;
            Vector curPage= ((Vector)curTable.getPages().get(indexP));
//            System.out.println(curPage);
//            System.out.println(indexP);
//            System.out.println((Vector)curTable.getPages());
            p = deSerializePage((String)curPage.get(0));
            int [] indexR = p.searchRecord(r);
            if (indexR[1]==1){
                throw new DBAppException("Clustering Key already Exists");
            }
            else {
                p.getTuples().add(indexR[0],r);            // could be optimised bcuz we are searching twice in the records
                p.setMax(((Record)p.getTuples().get(p.getTuples().size()-1)).getData().get(0).getValue());
                p.setMin(((Record)p.getTuples().get(0)).getData().get(0).getValue());
                p.setNoRows(p.getTuples().size());
//                System.out.println("number of records"+p.getTuples().size());
//                System.out.println("max records in page"+p.getMaxPage());
                if (p.getTuples().size()>p.getMaxPage()){
                    flag = true ;
//                    shifter = (Record)p.getTuples().get(p.getTuples().size()-1);
//                    p.getTuples().remove(shifter);
//                    p.setMax(((Record)p.getTuples().get(p.getTuples().size()-1)).getData().get(0).getValue());
//                    p.setNoRows(p.getTuples().size());
                }
                serializePage(p,(String)curPage.get(0));
                updateLocation((String)curPage.get(0),p,indexP,false);

            }
            if (flag){
                if (indexP < curTable.getPages().size()-1){
                    Vector nextPage= ((Vector)curTable.getPages().get(indexP+1));
                    Page pNext = null ;
                    pNext = deSerializePage((String)nextPage.get(0));
                    if (pNext.getTuples().size()< pNext.getMaxPage()){
                        shifter = (Record)p.getTuples().get(p.getTuples().size()-1);
                        p.getTuples().remove(shifter);
                        p.setMax(((Record)p.getTuples().get(p.getTuples().size()-1)).getData().get(0).getValue());
                        p.setNoRows(p.getTuples().size());
                        serializePage(p,(String)curPage.get(0));
                        updateLocation((String)curPage.get(0),p,indexP,false);
                        pNext.insertRecord(shifter);
                        pNext.setMax(((Record)pNext.getTuples().get(pNext.getTuples().size()-1)).getData().get(0).getValue());
                        pNext.setMin(((Record)pNext.getTuples().get(0)).getData().get(0).getValue());
                        pNext.setNoRows(pNext.getTuples().size());
                        serializePage(pNext,(String)nextPage.get(0));
                        updateLocation((String)nextPage.get(0),pNext,indexP+1,false);
                        flag = false ;
                    }

                }// end of our first if case not being last page
                if (flag && indexP > 0){
                    Vector prevPage= ((Vector)curTable.getPages().get(indexP-1));
                    Page pBack = null ;
                    pBack = deSerializePage((String)prevPage.get(0));
                    if (pBack.getTuples().size()< pBack.getMaxPage()){
                        shifter = (Record)p.getTuples().get(0);
                        p.getTuples().remove(shifter);
                        p.setMin(((Record)p.getTuples().get(0)).getData().get(0).getValue());
                        p.setNoRows(p.getTuples().size());
                        serializePage(p,(String)curPage.get(0));
                        updateLocation((String)curPage.get(0),p,indexP,false);
                        pBack.insertRecord(shifter);
                        pBack.setMax(((Record)pBack.getTuples().get(pBack.getTuples().size()-1)).getData().get(0).getValue());
                        pBack.setMin(((Record)pBack.getTuples().get(0)).getData().get(0).getValue());
                        pBack.setNoRows(pBack.getTuples().size());
                        serializePage(pBack,(String)prevPage.get(0));
                        updateLocation((String)prevPage.get(0),pBack,indexP-1,false);
                        flag = false ;
                    }

                  }// end of our second if not being first page

            }// end of our existing problem (SHIFTING!!)

            if (flag){   // new page needed at the end case  (overflow)
                shifter = (Record)p.getTuples().get(p.getTuples().size()-1);
                p.getTuples().remove(shifter);
                p.setMax(((Record)p.getTuples().get(p.getTuples().size()-1)).getData().get(0).getValue());
                p.setNoRows(p.getTuples().size());
                serializePage(p,(String)curPage.get(0));
                updateLocation((String)curPage.get(0),p,indexP,false);
                try {
                    Page newP = new Page(tableName);
                    newP.getTuples().add(shifter);
                    newP.setMax(shifter.getData().get(0).getValue());
                    newP.setMin(shifter.getData().get(0).getValue());
                    newP.setNoRows(newP.getTuples().size());
                    serializePage(newP,tableName+db.get(tableName).getPageID());
                    updateLocation(tableName+db.get(tableName).getPageID(),newP,indexP+1,true);
                    db.get(tableName).setPageID(db.get(tableName).getPageID()+1);
                }
                catch (Exception e ){
                    System.out.println("Something went wrong line 339");
                }

            } // end of overflow

        }// end of else

    }// end of insert method

    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {
        //checking if key already exists should be done when page is deSerialized
        if(tableName==null)
            throw new DBAppException("TABLE MUST HAVE A NAME!!!");

        //Table name exists
        if(!db.containsKey(tableName))
            throw new DBAppException("TABLE NAME NOT FOUND!!!");

        //No primary key
        if(clusteringKeyValue==null)
            throw new DBAppException("NO PRIMARY KEY SELECTED!!!");

        //Check if all columns exist
        Set <String> keys =columnNameValue.keySet();
        for(String key: keys) {
            if (!db.get(tableName).getColNameType().containsKey(key)) {
                throw new DBAppException("COLUMN NOT FOUND!!!");
            }
        }
        //check clustering key value matches type and range
        String clusteringKey = db.get(tableName).getClusteringKey();
        String clusteringKeyType = (String) db.get(tableName).getColNameType().get(clusteringKey);
        String clusteringKeyMin = (String) db.get(tableName).getColNameMin().get(clusteringKey);
        String clusteringKeyMax = (String) db.get(tableName).getColNameMax().get(clusteringKey);
        Object cVK = null ;
        if(clusteringKeyType.equals("java.lang.Integer")){
            boolean bound = false;
            try {
                if(Integer.parseInt(clusteringKeyValue)>=Integer.parseInt(clusteringKeyMin) && Integer.parseInt(clusteringKeyValue)<=Integer.parseInt(clusteringKeyMax))
                    bound = true;

                cVK =  new Integer(Integer.parseInt(clusteringKeyValue));

            }
            catch (Exception e){
                throw new DBAppException("Type mismatch: supposed to be an Integer.");
            }
            if(!bound)
                throw new DBAppException("Column value out of bounds");
        }
        else if(clusteringKeyType.equals("java.lang.Double")){
            boolean bound = false;
            try {
                if(Double.parseDouble(clusteringKeyValue)>=Double.parseDouble(clusteringKeyMin) && Double.parseDouble(clusteringKeyValue)<=Double.parseDouble(clusteringKeyMax))
                    bound = true;
                cVK = new Double (Double.parseDouble(clusteringKeyValue));
            }
            catch (Exception e){
                throw new DBAppException("Type mismatch: supposed to be a Double.");
            }
            if(!bound)
                throw new DBAppException("Column value out of bounds");
        }
        else if(clusteringKeyType.equals("java.util.Date")){
            boolean bound = false;
            try {
                Date currentValue = new SimpleDateFormat("yyyy-MM-dd").parse((String)clusteringKeyValue);
                Date currentMin = new SimpleDateFormat("yyyy-MM-dd").parse((String)clusteringKeyMin);
                Date currentMax = new SimpleDateFormat("yyyy-MM-dd").parse((String)clusteringKeyMax);
                if(currentValue.compareTo(currentMin)>=0 && currentValue.compareTo(currentMax)<=0)
                    bound = true;
                cVK = new SimpleDateFormat("yyyy-MM-dd").parse((String)clusteringKeyValue) ;
            }
            catch (Exception e){
                throw new DBAppException("Type mismatch: supposed to be a Date.");
            }
            if(!bound)
                throw new DBAppException("Column value out of bounds");
        }
        else if(clusteringKeyType.equals("java.lang.String")){
            boolean bound = false;
            try {
                if(clusteringKeyValue.compareToIgnoreCase(clusteringKeyMin)>=0 && clusteringKeyValue.compareToIgnoreCase(clusteringKeyMax)<=0)
                    bound = true;
                cVK =  new String (clusteringKeyValue) ;
            }
            catch (Exception e){
                throw new DBAppException("Type mismatch: supposed to be a String.");
            }
            if(!bound)
                throw new DBAppException("Column value out of bounds");
        }
        else {
            throw new DBAppException("Type mismatch");
        }


        //check every value is in the correct type and in range
        for(Map.Entry m: columnNameValue.entrySet()){
            String entryType = (String) db.get(tableName).getColNameType().get(m.getKey());
            if(entryType.equals("java.lang.Integer")){
                boolean bound = false;
                try {
                    int currentValue = ((Integer) m.getValue());
                    int currentMin = Integer.parseInt((String) db.get(tableName).getColNameMin().get(m.getKey()));
                    int currentMax = Integer.parseInt((String) db.get(tableName).getColNameMax().get(m.getKey()));
                    if(currentValue>=currentMin && currentValue<=currentMax)
                        bound = true;
                }
                catch (Exception e){
                    throw new DBAppException("Type mismatch: supposed to be an Integer.");
                }
                if(!bound)
                    throw new DBAppException("Column value out of bounds");
            }
            else if(entryType.equals("java.lang.Double")){
                boolean bound = false;
                try {
                    double currentValue = (double)m.getValue();
                    double currentMin = Double.parseDouble((String) db.get(tableName).getColNameMin().get(m.getKey()));
                    double currentMax = Double.parseDouble((String) db.get(tableName).getColNameMax().get(m.getKey()));
                    if(currentValue>=currentMin && currentValue<=currentMax)
                        bound = true;
                }
                catch (Exception e){
                    throw new DBAppException("Type mismatch: supposed to be a Double.");
                }
                if(!bound)
                    throw new DBAppException("Column value out of bounds");
            }
            else if(entryType.equals("java.util.Date")){
                boolean bound = false;
                try {
                    Date currentValue = ((Date)m.getValue());
//                    currentValue.setYear(currentValue.getYear()-1900);
//                    currentValue.setMonth(currentValue.getMonth()-1);
                    Date currentMin = new SimpleDateFormat("yyyy-MM-dd").parse((String) db.get(tableName).getColNameMin().get(m.getKey()));
                    Date currentMax = new SimpleDateFormat("yyyy-MM-dd").parse((String) db.get(tableName).getColNameMax().get(m.getKey()));
                    if(currentValue.compareTo(currentMin)>=0 && currentValue.compareTo(currentMax)<=0)
                        bound = true;
                }
                catch (Exception e){
                    e.printStackTrace();
//                    throw new DBAppException("Type mismatch: supposed to be a Date.");
                }
                if(!bound)
                    throw new DBAppException("Column value out of bounds");
            }
            else if(entryType.equals("java.lang.String")){
                boolean bound = false;
                try {
                    String currentValue = (String)m.getValue();
                    String currentMin = (String) db.get(tableName).getColNameMin().get(m.getKey());
                    String currentMax = (String) db.get(tableName).getColNameMax().get(m.getKey());
                    if(currentValue.compareToIgnoreCase(currentMin)>=0 && currentValue.compareToIgnoreCase(currentMax)<=0)
                        bound = true;
                }
                catch (Exception e){
                    throw new DBAppException("Type mismatch: supposed to be a String.");
                }
                if(!bound)
                    throw new DBAppException("Column value out of bounds");
            }
            else {
                throw new DBAppException("Type mismatch");
            }
        }

        //Check if there are no pages in the table
        if(db.get(tableName).getPages().size() == 0){
            throw new DBAppException("TABLE IS EMPTY");
        }
        //check if record clustering key exists

        columnNameValue.put((String)db.get(tableName).getClusteringKey(),cVK);

        Record r = new Record(columnNameValue,(String) db.get(tableName).getClusteringKey());

        int indexP = db.get(tableName).searchPage(r);
        Vector curPage = ((Vector)db.get(tableName).getPages().get(indexP));
        Page p = deSerializePage((String)curPage.get(0));
        int[] indexR = p.searchRecord(r);
        if (indexR[1]==0){
//            System.out.println("Record not found");
            throw new DBAppException("Record not found");
        }
        else {
            Vector<Pair> oldV = ((Record) p.getTuples().get(indexR[0])).getData();
            //update every Pair's value
            for (int i = 0; i < oldV.size(); i++) {
                if (columnNameValue.containsKey(oldV.get(i).getKey())) {
                    oldV.get(i).setValue(columnNameValue.get(oldV.get(i).getKey()));
                }
            }

            ((Record) p.getTuples().get(indexR[0])).setData(oldV);
            serializePage(p,(String)curPage.get(0));
            updateLocation((String)curPage.get(0),p,indexP,false);
        }

    }

    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        //Table name entered is null
        if(tableName==null)
            throw new DBAppException("TABLE MUST HAVE A NAME!!!");

        //Table name doesn't exist
        if(!db.containsKey(tableName))
            throw new DBAppException("TABLE NAME NOT FOUND!!!");

        //Check if column doesn't exist
        boolean containsClusteringKey = false;
        Set <String> keys =columnNameValue.keySet();
        for(String key: keys) {
            if (!db.get(tableName).getColNameType().containsKey(key)) {
                throw new DBAppException("COLUMN NOT FOUND!!!");
            }
            if(db.get(tableName).getClusteringKey().equals(key)){
                containsClusteringKey = true;
            }
        } // end of for loop

        //Check if there are no pages in the table
        if(db.get(tableName).getPages().size() == 0){
            throw new DBAppException("TABLE IS EMPTY");
        }

        String clusteringKey = db.get(tableName).getClusteringKey();
        if(containsClusteringKey){
            Record r = new Record(columnNameValue, clusteringKey);
            int indexP = db.get(tableName).searchPage(r);
            Vector curPage = ((Vector)db.get(tableName).getPages().get(indexP));
            Page p = deSerializePage((String)curPage.get(0));
            int[] indexR = p.searchRecord(r);
            if (indexR[1]==0){
//                System.out.println("Record not found");
                throw new DBAppException("Record not found");
            }
            else{
                for (int ii = 0 ;ii < r.getData().size();ii++){
                    if (columnNameValue.containsKey(r.getData().get(ii).getKey())){
                        if (!columnNameValue.get(r.getData().get(ii).getKey()).equals(r.getData().get(ii).getValue())){
                            throw  new DBAppException("Criteria not met");
                        }
                    }
                }
                p.getTuples().remove(indexR[0]);
                if(p.getTuples().size()==0){
                    db.get(tableName).getPages().remove(indexP);
                    try
                    {
                        //Saving of object in a file
                        FileOutputStream file = new FileOutputStream("src/main/resources/data/"+ tableName +".ser");
                        ObjectOutputStream out = new ObjectOutputStream(file);
                        // Method for serialization of object
                        Vector pages = (Vector)db.get(tableName).getPages();
                        out.writeObject(pages);
                        out.close();
                        file.close();
//                        System.out.println("Object has been serialized");
                    }
                    catch(IOException ex)
                    {
                        ex.printStackTrace();
                    }
                    File tbd = new File("src/main/resources/data/"+(String)curPage.get(0)+".ser");
                    tbd.delete();
                }
                else {
                    serializePage(p, (String) curPage.get(0));
                    updateLocation((String) curPage.get(0), p, indexP,false);
                }
            }
        }
        else{
            Record r = new Record(columnNameValue, clusteringKey);
            for (int i=db.get(tableName).getPages().size()-1; i>=0; i--){
                Vector curPage = ((Vector)db.get(tableName).getPages().get(i));
                Page p = deSerializePage((String)curPage.get(0));
                p.deleteRecord(r);
                if(p.getTuples().size()==0){
                    db.get(tableName).getPages().remove(i);
                    try
                    {
                        //Saving of object in a file
                        FileOutputStream file = new FileOutputStream("src/main/resources/data/"+ tableName +".ser");
                        ObjectOutputStream out = new ObjectOutputStream(file);
                        // Method for serialization of object
                        Vector pages = (Vector)db.get(tableName).getPages();
                        out.writeObject(pages);
                        out.close();
                        file.close();
//                System.out.println("Object has been serialized");
                    }
                    catch(IOException ex)
                    {

                        ex.printStackTrace();
                    }
                    File tbd = new File("src/main/resources/data/"+(String)curPage.get(0)+".ser");
                    tbd.delete();
                }
                else {
                    serializePage(p, (String) curPage.get(0));
                    updateLocation((String) curPage.get(0), p, i,false);
                }
            }
        }

    } //end of method

    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        return null;
    }



    public void serializePage(Object object, String filename){
        try
        {
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/data/"+filename + ".ser");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(object);

            out.close();
            file.close();

//            System.out.println("Object has been serialized");
        }

        catch(IOException ex)
        {
            ex.printStackTrace();
        }
    }

    public Page deSerializePage(String filename){
        Page output = null;

        try
        {
            // Reading the object from a file
            FileInputStream file = new FileInputStream("src/main/resources/data/"+filename+".ser");
            ObjectInputStream in = new ObjectInputStream(file);

            // Method for deserialization of object
            output = (Page)in.readObject();

            in.close();
            file.close();

//            System.out.println("Object has been deserialized ");
        }

        catch(IOException ex)
        {
            System.out.println("IOException is caught");
        }
        catch(ClassNotFoundException ex)
        {
            System.out.println("ClassNotFoundException is caught");
        }
        return output;
    }

    //Method to update Page location
    public void updateLocation(String location,Page page , int index , boolean f){
        //flag to create new page

        //Get Table
        DBTable curTable= db.get(page.getTable());
        Vector <Vector> pages= curTable.getPages();
        Vector pageInfo= new Vector();
        pageInfo.add(location);
        pageInfo.add(page.getMax());
        pageInfo.add(page.getMin());
        pageInfo.add(curTable.getName());
        pageInfo.add(page.getTuples().size());
        pageInfo.add(db.get(page.getTable()).getPageID());
        if (f){
            pages.add(index ,pageInfo);
        }
        else {
            try {
                pages.set(index, pageInfo);
            } catch (ArrayIndexOutOfBoundsException e) {
                pages.add(pageInfo);
            }
        }
        try
        {
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/data/"+ curTable.getName() + ".ser");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(pages);

            out.close();
            file.close();

//            System.out.println("Object has been serialized");
        }

        catch(IOException ex)
        {
            ex.printStackTrace();
        }
    }

    public  void readLocation(String tableName) throws IOException {
             Vector<Vector> pages=null;
        try
        {
            // Reading the object from a file
            FileInputStream file = new FileInputStream("src/main/resources/data/"+tableName+".ser");
            ObjectInputStream in = new ObjectInputStream(file);

            // Method for deserialization of object
            pages = (Vector<Vector>) in.readObject();

            in.close();
            file.close();

//            System.out.println("Object has been deserialized ");
        }

        catch(IOException ex)
        {
            //if nothing is found just exit
            return;
        }
        catch(ClassNotFoundException ex)
        {
            System.out.println("ClassNotFoundException is caught");
        }

            //add it to proper table
            db.get(tableName).setPages(pages);
        int max = 0 ;
        for (int i = 0 ; i <pages.size();i++){
            if ((int)pages.get(i).get(5)>=max){
                max = (int)pages.get(i).get(5);
            }
        }
            db.get(tableName).setPageID(max+1);
        }


public void validate(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
    //Table name entered is null
    if(tableName==null)
        throw new DBAppException("TABLE MUST HAVE A NAME!!!");

    //Table name doesn't exist
    if(!db.containsKey(tableName))
        throw new DBAppException("TABLE NAME NOT FOUND!!!");

    //No primary key
    if(!colNameValue.containsKey(db.get(tableName).getClusteringKey()) && colNameValue.get(db.get(tableName).getClusteringKey())!=null)
        throw new DBAppException("NO PRIMARY KEY SELECTED!!!");

    //Check if column doesn't exist
    Set <String> keys =colNameValue.keySet();
    for(String key: keys) {
        if (!db.get(tableName).getColNameType().containsKey(key)) {
            throw new DBAppException("COLUMN NOT FOUND!!!");
        }
    }

    //Check if no of columns matches table
    if(db.get(tableName).getColNameType().size()!=colNameValue.size())
        throw new DBAppException("Sizes of columns are incompatible");


    for(Map.Entry m: colNameValue.entrySet()){
        String entryType = (String) db.get(tableName).getColNameType().get(m.getKey());
        if(entryType.equals("java.lang.Integer")){
            boolean bound = false;
            try {
                int currentValue = ((Integer)m.getValue());
                int currentMin = Integer.parseInt((String) db.get(tableName).getColNameMin().get(m.getKey()));
                int currentMax = Integer.parseInt((String) db.get(tableName).getColNameMax().get(m.getKey()));
                if(currentValue>=currentMin && currentValue<=currentMax)
                    bound = true;
            }
            catch (Exception e){
                throw new DBAppException("Type mismatch: supposed to be an Integer.");
            }
            if(!bound)
                throw new DBAppException("Column value out of bounds");
        }
        else if(entryType.equals("java.lang.Double")){
            boolean bound = false;
            try {
                double currentValue = (double)m.getValue();
                double currentMin = Double.parseDouble((String) db.get(tableName).getColNameMin().get(m.getKey()));
                double currentMax = Double.parseDouble((String) db.get(tableName).getColNameMax().get(m.getKey()));
                if(currentValue>=currentMin && currentValue<=currentMax)
                    bound = true;
            }
            catch (Exception e){
                throw new DBAppException("Type mismatch: supposed to be a Double.");
            }
            if(!bound)
                throw new DBAppException("Column value out of bounds");
        }
        else if(entryType.equals("java.util.Date")){
            boolean bound = false;
            try {
                Date currentValue = ((Date)m.getValue());
//                currentValue.setYear(currentValue.getYear()-1900);
//                currentValue.setMonth(currentValue.getMonth()-1);
                Date currentMin = new SimpleDateFormat("yyyy-MM-dd").parse((String) db.get(tableName).getColNameMin().get(m.getKey()));
                Date currentMax = new SimpleDateFormat("yyyy-MM-dd").parse((String) db.get(tableName).getColNameMax().get(m.getKey()));

                if(currentValue.compareTo(currentMin)>=0 && currentValue.compareTo(currentMax)<=0)
                    bound = true;

            }
            catch (Exception e){
                throw new DBAppException("Type mismatch: supposed to be a Date.");
            }
            if(!bound)
                throw new DBAppException("Column value out of bounds");
        }
        else if(entryType.equals("java.lang.String")){
            boolean bound = false;
            try {
                String currentValue = (String)m.getValue();
                String currentMin = (String) db.get(tableName).getColNameMin().get(m.getKey());
                String currentMax = (String) db.get(tableName).getColNameMax().get(m.getKey());
//                    System.out.println(currentMin+"----"+currentMax);
                if(currentValue.compareToIgnoreCase(currentMin)>=0 && currentValue.compareToIgnoreCase(currentMax)<=0)
                    bound = true;
            }
            catch (Exception e){
                throw new DBAppException("Type mismatch: supposed to be a String.");
            }
            if(!bound)
                throw new DBAppException("Column value out of bounds");
        }
        else {
            throw new DBAppException("Type mismatch");
        }
    }
}

public void validate(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
    boolean hasCluster=false;

    if(clusteringKey==null)
        throw new DBAppException("TABLE MUST HAVE A CLUSTERING KEY!!!");
    if(tableName==null)
        throw new DBAppException("TABLE MUST HAVE A NAME!!!");

    for(Map.Entry m: colNameType.entrySet()){
        if(!(m.getValue().equals("java.lang.Integer")) && !(m.getValue().equals("java.lang.String")) && !(m.getValue().equals("java.lang.Double")) && !(m.getValue().equals("java.util.Date")) ) {
            throw new DBAppException(m.getValue()+ " IS NOT A VALID TYPE!!!");
        }
        if(m.getKey().equals(clusteringKey))
            hasCluster=true;
    }
    if(!hasCluster)
        throw new DBAppException("TABLE MUST HAVE A CLUSTERING KEY!!!");
    if(colNameType.size()!=colNameMin.size() || colNameType.size()!=colNameMax.size() || colNameMin.size()!=colNameMax.size())
        throw new DBAppException("Sizes of columns are incompatible");

    for(Map.Entry m: colNameType.entrySet()){
        if(!colNameMin.containsKey(m.getKey()) || !colNameMax.containsKey(m.getKey()))
            throw new DBAppException("Table content mismatch");
    }
}


   // update specific cell in csv MAYBE
//    public static void updateCSV(String fileToUpdate, String replace,
//                                 int row, int col) throws IOException {
//
//        File inputFile = new File(fileToUpdate);
//
//// Read existing file
//        CSVReader reader = new CSVReader(new FileReader(inputFile), ',');
//        List<String[]> csvBody = reader.readAll();
//// get CSV row column  and replace with by using row and column
//        csvBody.get(row)[col] = replace;
//        reader.close();
//
//// Write to CSV file which is open
//        CSVWriter writer = new CSVWriter(new FileWriter(inputFile), ',');
//        writer.writeAll(csvBody);
//        writer.flush();
//        writer.close();
//    }

    //       else{
//            //LOOP AND CHECK TYPE AND LOCATION THEN INSERT AND RETURN, IF NO LOOP ENDED WITHOUT RETURNING THEN NEW PAGE
//            for (int i = 0; i < curTable.getPages().size(); i++) {
//                Vector curPage= ((Vector)curTable.getPages().get(i));
//                Object max= curPage.get(1);
//                Object min= curPage.get(2);
//                Page p = null;
//                if(max instanceof Integer){
//                    //check if page has space
//                    if(((Integer)curPage.get(4)).compareTo(maxTuples)<0){
//                    //value in the middle
//                    if(((Integer)max).compareTo((Integer)curKey)>=0 && (((Integer)min).compareTo((Integer)curKey))<=0){
//                            p = deSerializePage((String) curPage.get(0));
//                            Record r = new Record(colNameValue,(String) clusteringKey);
//                            if(p.getTuples().contains(r)){
//                                throw new DBAppException("Clustering Key already exists in table");
//                            }
//                            p.insertRecord(r);
//                            serializePage(p,(String) curPage.get(0));
//                            ((String) curPage.get(0),p);
//                            return;
//                    //value is new max
//                    }else  if(((Integer)max).compareTo((Integer)curKey)<0 ){
//                        p= deSerializePage((String) curPage.get(0));
//                        p.setMax(curKey);
//                        Record r = new Record(colNameValue,(String) clusteringKey);
//                        p.insertRecord(r);
//                        serializePage(p,(String) curPage.get(0));
//                        updateLocation((String) curPage.get(0),p);
//                        return;
//                     //value is new min
//                    }else if((((Integer)min).compareTo((Integer)curKey))>0){
//                        p= deSerializePage((String) curPage.get(0));
//                        p.setMin(curKey);
//                        Record r = new Record(colNameValue,(String) clusteringKey);
//                        p.insertRecord(r);
//                        serializePage(p,(String) curPage.get(0));
//                        updateLocation((String) curPage.get(0),p);
//                        return;
//                    }
//                    }
//                }
//                if(max instanceof Double){
//                    //check if page has space
//                    if(((Integer)curPage.get(4)).compareTo(maxTuples)<0){
//                        //value in the middle
//                        if(((Double)max).compareTo((Double)curKey)>=0 && (((Double)min).compareTo((Double)curKey))<=0){
//                            p = deSerializePage((String) curPage.get(0));
//                            Record r = new Record(colNameValue,(String) clusteringKey);
//                            if(p.getTuples().contains(r)){
//                                throw new DBAppException("Clustering Key already exists in table");
//                            }
//                            p.insertRecord(r);
//                            serializePage(p,(String) curPage.get(0));
//                            updateLocation((String) curPage.get(0),p);
//                            return;
//                            //value is new max
//                        }else  if(((Double)max).compareTo((Double)curKey)<0 ){
//                            p= deSerializePage((String) curPage.get(0));
//                            p.setMax(curKey);
//                            Record r = new Record(colNameValue,(String) clusteringKey);
//                            p.insertRecord(r);
//                            serializePage(p,(String) curPage.get(0));
//                            updateLocation((String) curPage.get(0),p);
//                            return;
//                            //value is new min
//                        }else if((((Double)min).compareTo((Double)curKey))>0){
//                            p= deSerializePage((String) curPage.get(0));
//                            p.setMin(curKey);
//                            Record r = new Record(colNameValue,(String) clusteringKey);
//                            p.insertRecord(r);
//                            serializePage(p,(String) curPage.get(0));
//                            updateLocation((String) curPage.get(0),p);
//                            return;
//                        }
//                    }
//                }
//                if(max instanceof String){
//                    //check if page has space
//                    if(((Integer)curPage.get(4)).compareTo(maxTuples)<0){
//                        //value in the middle
//                        if(((String)max).compareTo((String)curKey)>=0 && (((String)min).compareTo((String)curKey))<=0){
//                            p = deSerializePage((String) curPage.get(0));
//                            Record r = new Record(colNameValue,(String) clusteringKey);
//                            if(p.getTuples().contains(r)){
//                                throw new DBAppException("Clustering Key already exists in table");
//                            }
//                            p.insertRecord(r);
//                            serializePage(p,(String) curPage.get(0));
//                            updateLocation((String) curPage.get(0),p);
//                            return;
//                            //value is new max
//                        }else  if(((String)max).compareTo((String)curKey)<0 ){
//                            p= deSerializePage((String) curPage.get(0));
//                            p.setMax(curKey);
//                            Record r = new Record(colNameValue,(String) clusteringKey);
//                            p.insertRecord(r);
//                            serializePage(p,(String) curPage.get(0));
//                            updateLocation((String) curPage.get(0),p);
//                            return;
//                            //value is new min
//                        }else if((((String)min).compareTo((String)curKey))>0){
//                            p= deSerializePage((String) curPage.get(0));
//                            p.setMin(curKey);
//                            Record r = new Record(colNameValue,(String) clusteringKey);
//                            p.insertRecord(r);
//                            serializePage(p,(String) curPage.get(0));
//                            updateLocation((String) curPage.get(0),p);
//                            return;
//                        }
//                    }
//                }
//                if(max instanceof Date){
//                    //check if page has space
//                    if(((Integer)curPage.get(4)).compareTo(maxTuples)<0){
//                        //value in the middle
//                        if(((Date)max).compareTo((Date)curKey)>=0 && (((Date)min).compareTo((Date)curKey))<=0){
//                            p = deSerializePage((String) curPage.get(0));
//                            Record r = new Record(colNameValue,(String) clusteringKey);
//                            if(p.getTuples().contains(r)){
//                                throw new DBAppException("Clustering Key already exists in table");
//                            }
//                            p.insertRecord(r);
//                            serializePage(p,(String) curPage.get(0));
//                            updateLocation((String) curPage.get(0),p);
//                            return;
//                            //value is new max
//                        }else  if(((Date)max).compareTo((Date)curKey)<0 ){
//                            p= deSerializePage((String) curPage.get(0));
//                            p.setMax(curKey);
//                            Record r = new Record(colNameValue,(String) clusteringKey);
//                            p.insertRecord(r);
//                            serializePage(p,(String) curPage.get(0));
//                            updateLocation((String) curPage.get(0),p);
//                            return;
//                            //value is new min
//                        }else if((((Date)min).compareTo((Date)curKey))>0){
//                            p= deSerializePage((String) curPage.get(0));
//                            p.setMin(curKey);
//                            Record r = new Record(colNameValue,(String) clusteringKey);
//                            p.insertRecord(r);
//                            serializePage(p,(String) curPage.get(0));
//                            updateLocation((String) curPage.get(0),p);
//                            return;
//                        }
//                    }
//                }
//            }
//            //PAGES FULL CREATE A NEW ONE
//            try {
//                Page p = new Page(tableName);
//                p.setMax(curKey);
//                p.setMin(curKey);
//                Record r = new Record(colNameValue,(String) clusteringKey);
//                p.insertRecord(r);
//                serializePage(p,tableName+curTable.getPages().size());
//                updateLocation(tableName+curTable.getPages().size(),p);
//                //System.out.println(curTable.getPages());
//            }
//            catch(IOException e){
//                e.printStackTrace();
//            }
//        }





            /// to defrag all pages and shift

//            while (flag && ++indexP < curTable.getPages().size()){
//
//                    curPage= ((Vector)curTable.getPages().get(indexP));
//                    p = deSerializePage((String)curPage.get(0));
//                    p.getTuples().add(0,shifter);
//                    p.setMax(((Record)p.getTuples().get(p.getTuples().size()-1)).getData().get(0).getValue());
//                    p.setMin(((Record)p.getTuples().get(0)).getData().get(0).getValue());
//                    p.setNoRows(p.getTuples().size());
//                    if (p.getTuples().size()<=p.getMaxPage()){
//                         flag = false ;
//                         p.setMax(((Record)p.getTuples().get(p.getTuples().size()-1)).getData().get(0).getValue());
//                         p.setNoRows(p.getTuples().size());
//                         serializePage(p,(String)curPage.get(0));
//                         updateLocation((String)curPage.get(0),p,indexP);
//                         break;
//                        }
//                    shifter = (Record)p.getTuples().get(p.getTuples().size()-1);
//                    p.getTuples().remove(shifter);
//                    p.setMax(((Record)p.getTuples().get(p.getTuples().size()-1)).getData().get(0).getValue());
//                    p.setNoRows(p.getTuples().size());
//
//                    serializePage(p,(String)curPage.get(0));
//                    updateLocation((String)curPage.get(0),p,indexP);
//
//
//            }










}
