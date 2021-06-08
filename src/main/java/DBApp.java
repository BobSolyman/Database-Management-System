import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class DBApp implements DBAppInterface{

     private HashMap<String,DBTable> db= new HashMap();
     //Vector [Table,ColName,location]
    private int bucketSize ;
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
        try {
            Properties prop = new Properties();
            FileInputStream property = new FileInputStream("src/main/resources/DBApp.config");
            prop.load(property);
            bucketSize = Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));
        }catch(Exception e){
            e.printStackTrace();
        }
        File theDir = new File("src/main/resources/data");
        if (!theDir.exists()) {
            theDir.mkdirs();
        }
        try {
            if(getFileSize("src/main/resources/metadata.csv")>0){
            db = getMap("src/main/resources/metadata.csv");

            } else {
                FileWriter fr = new FileWriter("src/main/resources/metadata.csv");
                BufferedWriter br2 = new BufferedWriter(fr);
                String d = "Table Name, Column Name, Column Type, ClusteringKey, Indexed , min , max"+"\n";
                br2.write(d);
                br2.close();
                fr.close();
            }

                for (String table : db.keySet()) {
                    readLocation(table);
                }


        } catch (IOException i) {
            i.printStackTrace();
            System.out.println("NO PREVIOUS DB!!!");
            return;
        }



        Set <String> keys =db.keySet();
        for(String key: keys){
            File grids = new File("src/main/resources/"+key+"Grids");
            if (!grids.exists()) {
                db.get(key).setGrids(new Hashtable<Vector<String>, String>());
            }
            else{
                try {
                    db.get(key).setGrids((Hashtable<Vector<String>, String>) deSerialize(key+"Grids"));
                }
                catch (Exception e){
                    System.out.println("GRIDS NOT FOUND");
                }
            }

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
        //exceptions
        if(tableName==null)
            throw new DBAppException("TABLE MUST HAVE A NAME!!!");

        //Table name doesn't exist
        if(!db.containsKey(tableName))
            throw new DBAppException("TABLE NAME NOT FOUND!!!");

        //check colName exist
        for(String columnName: columnNames){
            if(!db.get(tableName).getColNameType().containsKey(columnName)){
                throw new DBAppException("COLUMN DOES NOT EXIST!!!");
            }
        }

        //handle order of columnNames for grid

        Vector<String> rearranged = new Vector<>();
        rearranged.addAll(Arrays.asList(columnNames));
        Collections.sort(rearranged);
        DBTable currentTable= db.get(tableName);
        if(currentTable.getGrids().containsKey(rearranged)){
            System.out.println("Grid already exists");
            return;
        }

        Grid grid= new Grid(tableName,columnNames,currentTable.getColNameMin(),currentTable.getColNameMax(),currentTable.getColNameType());
        String clusteringKey=  (String) currentTable.getClusteringKey();
        Vector<Vector> pages= currentTable.getPages();

        for(Vector page : pages) {
            Page currentPage = deSerializePage((String) page.get(0));
            Vector<Record> tuples = currentPage.getTuples();

            for (Record r : tuples){
                Vector<Integer> loc = grid.getIndex(r);
                bucketEntry bE = new bucketEntry(r,(String) page.get(0));
                if (grid.getBuckets().containsKey(loc)){
                    Cell vB = (Cell) deSerialize(grid.getGridID()+loc.toString());
                    int indexB = vB.searchBuckets(bE);
                    Bucket b = vB.getBuckets().get(indexB);
                    int indexBE = b.searchBucketEntry(bE);
                    b.getEntries().add(indexBE, bE);
                    b.updateMinMax(bE);
                    if(b.getEntries().size()>b.getMaxBucket()){
                        boolean flag = false;
                        for(int i=indexB; i<vB.getBuckets().size()-1; i++){
                            bucketEntry lastBE = vB.getBuckets().get(indexB).getEntries().get(vB.getBuckets().get(indexB).getEntries().size()-1);
                            Bucket bb = vB.getBuckets().get(indexB+1);
                            bb.getEntries().add(bb.searchBucketEntry(lastBE), lastBE);
                            bb.updateMinMax(lastBE);
                            if(bb.getEntries().size()<=bb.getMaxBucket()){
                                flag = true;
                                break;
                            }
                        }
                        if(!flag){
                            bucketEntry lastBE = vB.getBuckets().get(vB.getBuckets().size()-1).getEntries().get(vB.getBuckets().get(vB.getBuckets().size()-1).getEntries().size()-1);
                            Bucket bb = null;
                            try {
                                bb = new Bucket(grid.getType(), grid.getColumns());
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            bb.getEntries().add(0, lastBE);
                            bb.updateMinMax(lastBE);
                            vB.getBuckets().add(bb);
                        }
                    }
                    grid.getBuckets().put(loc,vB.getBucketID());
                    serialize(vB,vB.getBucketID());
                    // handling the overflow buckets !!!
                }
                else {
                    //we need to create our first bucket here :)
                    Cell vB = new Cell(grid.getGridID()+loc.toString(), grid.getColumns(), grid.getType());
                    try {
                        Bucket b = new Bucket(grid.getType(), grid.getColumns());
                        b.getEntries().add(bE);
                        b.updateMinMax(bE);
                        vB.getBuckets().add(b);

                        grid.getBuckets().put(loc,vB.getBucketID());
                        serialize(vB,vB.getBucketID());

                    }
                    catch (IOException e){
                    }

                }

            }

        }
        // saving the grid and serializing it
        currentTable.addGrid(grid);
        serialize(grid, (String)grid.getGridID());
        serialize(currentTable.getGrids(), tableName+"Grids");
    }// end of method

    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException, ParseException {

        validate(tableName,colNameValue);

        //Get the table and find the clustering key
        DBTable curTable= db.get(tableName);
        Object clusteringKey= curTable.getClusteringKey();
        Object curKey= colNameValue.get(clusteringKey);
        String pageLoc = "";
        Record r = null;
        //Check if there are no pages in the table
        if(curTable.getPages().size() == 0){
            try {
                Page p = new Page(tableName);
                r = new Record(colNameValue,(String) clusteringKey);
                p.getTuples().add(r);
                p.setMax(r.getData().get(0).getValue());
                p.setMin(r.getData().get(0).getValue());
                p.setNoRows(p.getTuples().size());
                pageLoc = tableName+db.get(tableName).getPageID() ;
                serializePage(p,tableName+db.get(tableName).getPageID());
                updateLocation(tableName+db.get(tableName).getPageID(),p,0,false);
                db.get(tableName).setPageID(db.get(tableName).getPageID()+1);
            }
            catch(IOException e){
                e.printStackTrace();
            }
        }

        else {
            r = new Record(colNameValue,(String) clusteringKey);
            int indexP = curTable.searchPage(r);
            boolean flag = false ;
            Page p = null ;
            Record shifter = null ;
            Vector curPage= ((Vector)curTable.getPages().get(indexP));
            p = deSerializePage((String)curPage.get(0));
            int [] indexR = p.searchRecord(r);
            if (indexR[1]==1){
                throw new DBAppException("Clustering Key already Exists");
            }
            else {
                p.getTuples().add(indexR[0],r);
                p.setMax(((Record)p.getTuples().get(p.getTuples().size()-1)).getData().get(0).getValue());
                p.setMin(((Record)p.getTuples().get(0)).getData().get(0).getValue());
                p.setNoRows(p.getTuples().size());
                if (p.getTuples().size()>p.getMaxPage()){
                    flag = true ;
                }
                pageLoc = (String)curPage.get(0);
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
                        if(shifter==r){
                            pageLoc = (String)nextPage.get(0);
                        }
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
                        if(shifter==r){
                            pageLoc = (String)prevPage.get(0);
                        }
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
                    if(shifter==r){
                        pageLoc = tableName+db.get(tableName).getPageID();
                    }
                    serializePage(newP,tableName+db.get(tableName).getPageID());
                    updateLocation(tableName+db.get(tableName).getPageID(),newP,indexP+1,true);
                    db.get(tableName).setPageID(db.get(tableName).getPageID()+1);
                }
                catch (Exception e ){
                    System.out.println("Something went wrong line 339");
                }

            } // end of overflow

        }// end of else

        //indexed
        Set<Vector<String>> gridIDs = curTable.getGrids().keySet();
        for(Vector<String> g: gridIDs){
            String ID = (String)curTable.getGrids().get(g);
            Grid grid = (Grid)deSerialize(ID);
            Vector<Integer> loc = grid.getIndex(r);
            bucketEntry bE = new bucketEntry(r,(String) pageLoc);
            if (grid.getBuckets().containsKey(loc)){
                Cell vB = (Cell) deSerialize(grid.getGridID()+loc.toString());
                int indexB = vB.searchBuckets(bE);
                Bucket b = vB.getBuckets().get(indexB);
                int indexBE = b.searchBucketEntry(bE);
                b.getEntries().add(indexBE, bE);
                b.updateMinMax(bE);
                if(b.getEntries().size()>b.getMaxBucket()){
                    boolean flag = false;
                    for(int i=indexB; i<vB.getBuckets().size()-1; i++){
                        bucketEntry lastBE = vB.getBuckets().get(indexB).getEntries().get(vB.getBuckets().get(indexB).getEntries().size()-1);
                        Bucket bb = vB.getBuckets().get(indexB+1);
                        bb.getEntries().add(bb.searchBucketEntry(lastBE), lastBE);
                        bb.updateMinMax(lastBE);
                        if(bb.getEntries().size()<=bb.getMaxBucket()){
                            flag = true;
                            break;
                        }
                    }
                    if(!flag){
                        bucketEntry lastBE = vB.getBuckets().get(vB.getBuckets().size()-1).getEntries().get(vB.getBuckets().get(vB.getBuckets().size()-1).getEntries().size()-1);
                        Bucket bb = null;
                        try {
                            bb = new Bucket(grid.getType(), grid.getColumns());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        bb.getEntries().add(0, lastBE);
                        bb.updateMinMax(lastBE);
                        vB.getBuckets().add(bb);
                    }
                }
                grid.getBuckets().put(loc,vB.getBucketID());
                serialize(vB,vB.getBucketID());
                // handling the overflow buckets !!!
            }
            else {
                //we need to create our first bucket here :)
                Cell vB = new Cell(grid.getGridID()+loc.toString(), grid.getColumns(), grid.getType());
                try {
                    Bucket b = new Bucket(grid.getType(), grid.getColumns());
                    b.getEntries().add(bE);
                    b.updateMinMax(bE);
                    vB.getBuckets().add(b);

                    grid.getBuckets().put(loc,vB.getBucketID());
                    serialize(vB,vB.getBucketID());

                }
                catch (IOException e){
                }

            }
        }
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
                    Date currentMin = new SimpleDateFormat("yyyy-MM-dd").parse((String) db.get(tableName).getColNameMin().get(m.getKey()));
                    Date currentMax = new SimpleDateFormat("yyyy-MM-dd").parse((String) db.get(tableName).getColNameMax().get(m.getKey()));
                    if(currentValue.compareTo(currentMin)>=0 && currentValue.compareTo(currentMax)<=0)
                        bound = true;
                }
                catch (Exception e){
                    e.printStackTrace();
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

        //by pages
        columnNameValue.put((String)db.get(tableName).getClusteringKey(),cVK);

        Record r = new Record(columnNameValue,(String) db.get(tableName).getClusteringKey());

        int indexP = db.get(tableName).searchPage(r);
        Vector curPage = ((Vector)db.get(tableName).getPages().get(indexP));
        Page p = deSerializePage((String)curPage.get(0));
        int[] indexR = p.searchRecord(r);
        Record gRecord = null;
        Record newR = null;
        String forGLoc = "";
        if (indexR[1]==0){
            throw new DBAppException("Record not found");
        }
        else {
            gRecord = (Record) p.getTuples().get(indexR[0]);
            Vector<Pair> oldV = gRecord.getData();
            //update every Pair's value
            for (int i = 0; i < oldV.size(); i++) {
                if (columnNameValue.containsKey(oldV.get(i).getKey())) {
                    oldV.get(i).setValue(columnNameValue.get(oldV.get(i).getKey()));
                }
            }

            newR = ((Record) p.getTuples().get(indexR[0]));
            newR.setData(oldV);

            for (Map.Entry m : columnNameValue.entrySet()){
                newR.getContent().put((String) m.getKey(),m.getValue());
            }
            forGLoc = (String)curPage.get(0);
            serializePage(p,(String)curPage.get(0));
            updateLocation((String)curPage.get(0),p,indexP,false);
        }


        //by indices
        Vector<String> columns = (Vector<String>) columnNameValue.keySet();
        Collections.sort(columns);
        Set<Vector<String>> Grids = db.get(tableName).getGrids().keySet();
        for(Vector<String> g: Grids){
            boolean flag = false;
            for(String column : columns){
                if(g.contains(column)){
                    flag = true;
                    break;
                }
            }
            if(flag){
                Grid grid =(Grid) deSerialize((String)db.get(tableName).getGrids().get(g));
                    Vector<Integer> gLoc = grid.getIndex(gRecord);
                    Cell cell = (Cell)deSerialize((String)grid.getBuckets().get(gLoc));
                    bucketEntry bE = new bucketEntry(gRecord, forGLoc);
                    int indexB = cell.searchBuckets(bE);
                    Bucket b = cell.getBuckets().get(indexB);
                    int indexBE = b.searchBucketEntry(bE);
                    b.getEntries().remove(indexBE);
                    b.updateMinMax(null);
                    serialize(cell, (String)grid.getBuckets().get(gLoc));
                    Vector<Integer> loc = grid.getIndex(r);
                    gLoc = grid.getIndex(newR);
                    bE = new bucketEntry(newR, forGLoc);
                    if (grid.getBuckets().containsKey(gLoc)){
                        cell = (Cell)deSerialize((String)grid.getBuckets().get(gLoc));
                        indexB = cell.searchBuckets(bE);
                        b = cell.getBuckets().get(indexB);
                        indexBE = b.searchBucketEntry(bE);
                        b.getEntries().add(indexBE, bE);
                        b.updateMinMax(bE);
                        if(b.getEntries().size()>b.getMaxBucket()){
                            boolean flag2 = false;
                            for(int i=indexB; i<cell.getBuckets().size()-1; i++){
                                bucketEntry lastBE = cell.getBuckets().get(indexB).getEntries().get(cell.getBuckets().get(indexB).getEntries().size()-1);
                                Bucket bb = cell.getBuckets().get(indexB+1);
                                bb.getEntries().add(bb.searchBucketEntry(lastBE), lastBE);
                                bb.updateMinMax(lastBE);
                                if(bb.getEntries().size()<=bb.getMaxBucket()){
                                    flag2 = true;
                                    break;
                                }
                            }
                            if(!flag2){
                                bucketEntry lastBE = cell.getBuckets().get(cell.getBuckets().size()-1).getEntries().get(cell.getBuckets().get(cell.getBuckets().size()-1).getEntries().size()-1);
                                Bucket bb = null;
                                try {
                                    bb = new Bucket(grid.getType(), grid.getColumns());
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                bb.getEntries().add(0, lastBE);
                                bb.updateMinMax(lastBE);
                                cell.getBuckets().add(bb);
                            }
                        }
                        grid.getBuckets().put(gLoc,cell.getBucketID());
                        serialize(cell,cell.getBucketID());
                        // handling the overflow buckets !!!
                    }
                    else {
                        //we need to create our first bucket here :)
                        Cell vB = new Cell(grid.getGridID()+loc.toString(), grid.getColumns(), grid.getType());
                        try {
                            Bucket bu = new Bucket(grid.getType(), grid.getColumns());
                            bu.getEntries().add(bE);
                            bu.updateMinMax(bE);
                            vB.getBuckets().add(bu);
                            grid.getBuckets().put(loc,vB.getBucketID());
                            serialize(vB,vB.getBucketID());

                        }
                        catch (IOException e){
                        }

                    }

                serialize(grid,grid.getGridID());
            }
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


        Vector<String> col = (Vector<String>) columnNameValue.keySet();
        Collections.sort(col);
        Set<Vector<String>> Grids = db.get(tableName).getGrids().keySet();
        Vector<String> commonCol = new Vector<>();
        for(Vector<String> m :Grids){
            if (col.containsAll(m)){
                if (m.size()>commonCol.size())
                    commonCol = m ;
            }
        }

        if(commonCol.size()>0){
            Grid grid = (Grid) deSerialize((String) db.get(tableName).getGrids().get(commonCol));
            Record r = new Record(columnNameValue, clusteringKey);
            Vector<Integer> gLoc = grid.getIndex(r);
            if(grid.getBuckets().containsKey(gLoc)){
                Cell cell = (Cell) deSerialize(grid.getGridID()+gLoc.toString());
                bucketEntry bE = new bucketEntry(r, "somewhere");
                int bucketCounter = cell.searchBuckets(bE);
                int entryCounter = cell.getBuckets().get(bucketCounter).searchBucketEntry(bE);
                boolean firstentry  =true;
                for(int j=bucketCounter; j<cell.getBuckets().size(); j++){
                    Bucket bb = cell.getBuckets().get(j);
                    boolean emptied = false;
                    for(int i=0; i<bb.getEntries().size(); i++){
                        if(firstentry){
                            i = entryCounter;
                            firstentry = false;
                        }
                        bucketEntry bee = bb.getEntries().get(i);
                        boolean belongs = true;
                        for(Map.Entry m : columnNameValue.entrySet()){
                            if(bee.getRow()!=null){
                                if(bee.getRow().getContent().get(m.getKey())!=m.getValue()){
                                    belongs = false;
                                    break;
                                }
                            }
                        }
                        if(belongs){
                            bucketEntry removed = bb.getEntries().get(i);
                            Page pp = deSerializePage(removed.getPageLoc());
                            pp.getTuples().remove(removed.getRow());

                            int indexP = db.get(tableName).searchPage(removed.getRow());
                            Vector curPage = ((Vector)db.get(tableName).getPages().get(indexP));
                            if(pp.getTuples().size()==0){
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
                                }
                                catch(IOException ex)
                                {
                                    ex.printStackTrace();
                                }
                                File tbd = new File("src/main/resources/data/"+(String)curPage.get(0)+".ser");
                                tbd.delete();
                            }
                            else {
                                serializePage(pp, (String) curPage.get(0));
                                updateLocation((String) curPage.get(0), pp, indexP,false);
                            }


                            bb.getEntries().remove(i);
                            if(bb.getEntries().size()==0){
                                emptied=true;
                            }
                            i--;
                        }

                    }
                    if(emptied){
                        cell.getBuckets().remove(j);
                        j--;
                    }
                    else{
                        bb.updateMinMax(null);
                    }
                }
                if(cell.getBuckets().size()==0){
                    grid.getBuckets().remove(gLoc);
                    File tbd = new File("src/main/resources/data/"+cell.getBucketID()+".ser");
                    tbd.delete();
                }
                else {
                    serialize(cell, cell.getBucketID());
                }
            }
        }

       else if(containsClusteringKey){
            Record r = new Record(columnNameValue, clusteringKey);
            int indexP = db.get(tableName).searchPage(r);
            Vector curPage = ((Vector)db.get(tableName).getPages().get(indexP));
            Page p = deSerializePage((String)curPage.get(0));
            int[] indexR = p.searchRecord(r);
            if (indexR[1]==0){
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
        }// end of searching using clusteringkey on pages
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
        }// end of brute force search on pages

    } //end of method

    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        Vector results= new Vector();
        Vector<String> columns= new Vector();
        DBTable table = null;
        for(int i=0;i<sqlTerms.length;i++){
            String tableName=sqlTerms[i]._strTableName;
            String colName=sqlTerms[i]._strColumnName;
            String operator=sqlTerms[i]._strOperator;
            Object val=sqlTerms[i]._objValue;
            if(!db.containsKey(tableName))
                throw new DBAppException("TABLE NAME NOT FOUND!!!");
            table=db.get(tableName);

            if(!table.getColNameType().containsKey(colName))
                throw new DBAppException("COLUMN DOES NOT EXIST!!!");

            if(!operator.equals(">")&&!operator.equals(">=")&&!operator.equals("<")&&!operator.equals("<=")&&!operator.equals("=")&&!operator.equals("!=")){
                throw new DBAppException("OPERATOR NOT SUPPORTED!!!");
            }

            columns.add(colName);
        }
        // we need to check on the operator !!!!!
        for (int i = 0 ; i < arrayOperators.length ; i++){
            if (!arrayOperators[i].equals("AND") && !arrayOperators[i].equals("OR") && !arrayOperators[i].equals("XOR")){
                throw new DBAppException("OPERATOR NOT SUPPORTED!!!");
            }
        }

        table = db.get(sqlTerms[0]._strTableName);

        Set<Vector<String>> gridIDs = table.getGrids().keySet();
        Vector<bucketEntry> op1 = new Vector<>();
        boolean firsttime = true;

        Vector<String> andedColumns = new Vector<>();
        boolean andFirst = true ;
        Vector<SQLTerm> andedTerms = new Vector<>();
        if(arrayOperators.length==0){
            op1 = getTerm(sqlTerms[0]._strTableName, sqlTerms[0]._strColumnName, sqlTerms[0]._objValue, sqlTerms[0]._strOperator);
            return op1.iterator();
        }
        for(int i=0; i<arrayOperators.length; i++){
            String t=sqlTerms[i]._strTableName;
            String col=sqlTerms[i]._strColumnName;
            String op=sqlTerms[i]._strOperator;
            Object val=sqlTerms[i]._objValue;
            String t2=sqlTerms[i+1]._strTableName;
            String col2=sqlTerms[i+1]._strColumnName;
            String op2=sqlTerms[i+1]._strOperator;
            Object val2=sqlTerms[i+1]._objValue;

            if(arrayOperators[i].equals("AND")){
                if (andFirst && op.equals("=") && op2.equals("=")){
                    andFirst = false ;
                    andedColumns.add(col);
                    andedTerms.add(sqlTerms[i]);
                    andedColumns.add(col2);
                    andedTerms.add(sqlTerms[i+1]);
                    int c = 1;
                    for ( c = i+1 ; c < arrayOperators.length ; c++){
                        if (arrayOperators[c].equals("AND") && sqlTerms[c+1]._strOperator.equals("=")){
                            andedColumns.add(col2);
                            andedTerms.add(sqlTerms[c+1]);
                        }
                        else {
                            break;
                        }
                    }
                    Collections.sort(andedColumns);
                    Set<Vector<String>> Grids = table.getGrids().keySet();
                    Vector<String> commonCol = new Vector<>();
                    for(Vector<String> m :Grids){
                        if (andedColumns.containsAll(m)){
                            if (m.size()>commonCol.size())
                                commonCol = m ;
                        }
                    }
                    if (commonCol.size()>1){
                        op1=getMultiDimTerm((Grid) deSerialize((String) table.getGrids().get(commonCol)),andedTerms, t2);
                        firsttime = false ;
                        i = c-1;
                    }
                    else {
                        if (firsttime) {
                            firsttime = false;
                            op1 = getTerm(t, col, val, op);
                        }
                        op1 = queryAND(op1, getTerm(t2, col2, val2, op2));
                    }


                }else {
                    if (firsttime) {
                        firsttime = false;
                        op1 = getTerm(t, col, val, op);
                    }
                    op1 = queryAND(op1, getTerm(t2, col2, val2, op2));
                }
            }
            else if(arrayOperators[i].equals("OR")){
                if(firsttime){
                    firsttime=false;
                    op1 = getTerm(t, col, val, op);
                }
                op1 = queryOR(op1, getTerm(t2, col2, val2, op2));
            }
            else if(arrayOperators[i].equals("XOR")){
                if(firsttime){
                    firsttime=false;
                    op1 = getTerm(t, col, val, op);
                }
                op1 = queryXOR(op1, getTerm(t2, col2, val2, op2));

            }
        }





        return op1.iterator();
    }

    private Vector<bucketEntry> getMultiDimTerm(Grid grid, Vector<SQLTerm> andedTerms,String tableName) {
        Vector<bucketEntry> BEs = new Vector<>();
        Hashtable <String , Object>  columnNameValue = new Hashtable<>();
        for (int i = 0 ; i<andedTerms.size();i++){
            SQLTerm x = andedTerms.get(i);
            columnNameValue.put(x._strColumnName,x._objValue);
        }
        Record r = new Record(columnNameValue, db.get(tableName).getClusteringKey());
        Vector<Integer> gLoc = grid.getIndex(r);
        if(grid.getBuckets().containsKey(gLoc)) {
            Cell cell = (Cell) deSerialize(grid.getGridID() + gLoc.toString());
            bucketEntry bE = new bucketEntry(r, "somewhere");
            int bucketCounter = cell.searchBuckets(bE);
            int entryCounter = cell.getBuckets().get(bucketCounter).searchBucketEntry(bE);
            boolean firsttime = true ;
            for (int i = bucketCounter ; i <cell.getBuckets().size();i++){

                for (int j = 0 ; j<cell.getBuckets().get(i).getEntries().size();j++){
                    if (firsttime){
                        firsttime = false ;
                        j = entryCounter;
                    }
                    bucketEntry bbE = cell.getBuckets().get(i).getEntries().get(j);
                    Record record = bbE.getRow();
                    for (SQLTerm x : andedTerms){
                        if (!x._objValue.equals(record.getContent().get(x._strColumnName))){
                            return BEs;
                        }
                    }
                    BEs.add(bbE);
                }

            }

        }

        return BEs;
    }

    public Vector<bucketEntry> getTerm(String tableName, String columnName, Object columnValue, String operator) {
        Vector<bucketEntry> BEs = new Vector<>();
        DBTable table = db.get(tableName);
        Vector<String> col = new Vector<>();
        col.add(columnName);
        if(table.getGrids().containsKey(col)){
            Grid g = (Grid) deSerialize((String)table.getGrids().get(col));
            Hashtable<String, Object> data = new Hashtable<>();
            data.put(columnName, columnValue);
            Record gRecord = new Record(data, table.getClusteringKey());
            Vector<Integer> gLoc = g.getIndex(gRecord);
                if (operator.equals("=")) {
                    if (g.getBuckets().containsKey(gLoc)) {
                        Cell cell = (Cell) deSerialize((String) g.getBuckets().get(gLoc));
                        bucketEntry bE = new bucketEntry(gRecord, "somewhere");
                        int indexB = cell.searchBuckets(bE);
                        Bucket b = cell.getBuckets().get(indexB);
                        int indexBE = b.searchBucketEntry(bE);
                    boolean firsttime = true;
                    for (int i = indexB; i < cell.getBuckets().size(); i++) {
                        Bucket bb = cell.getBuckets().get(i);
                        for (int j = 0; j < bb.getEntries().size(); j++) {
                            if (firsttime) {
                                firsttime = false;
                                j = indexBE;
                            }
                            bucketEntry bee = bb.getEntries().get(j);
                            if (bee.getRow() != null) {
                                if (bee.getRow().getContent().get(columnName).equals(columnValue)) {
                                    BEs.add(bee);
                                } else {
                                    return BEs;
                                }
                            }
                        }
                    }
                }
                    return BEs;
                } else if (operator.equals(">")) {
                    boolean firsttime = true;
                    boolean firsttimeCell = true;
                    Cell cell = (Cell) deSerialize((String) g.getBuckets().get(gLoc));
                    bucketEntry bE = new bucketEntry(gRecord, "somewhere");
                    int indexB = cell.searchBuckets(bE);
                    Bucket b = cell.getBuckets().get(indexB);
                    int indexBE = b.searchBucketEntry(bE);
                    for(int k=gLoc.get(0); k<10; k++) {
                        Vector<Integer> count = new Vector<>();
                        count.add(k);
                        if(g.getBuckets().containsKey(count)) {
                            cell = (Cell) deSerialize((String) g.getBuckets().get(count));
                            for (int i = 0; i < cell.getBuckets().size(); i++) {
                                if(firsttimeCell){
                                    firsttimeCell = false;
                                    i= indexB;
                                }
                                Bucket bb = cell.getBuckets().get(i);
                                for (int j = 0; j < bb.getEntries().size(); j++) {
                                    if (firsttime) {
                                        firsttime = false;
                                        j = indexBE+1;
                                    }
                                    bucketEntry bee = bb.getEntries().get(j);
                                    if (bee.getRow() != null) {
                                        if(!(bee.getRow().getContent().get(columnName).equals(columnValue))){
                                            BEs.add(bee);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    return  BEs;
                } else if (operator.equals(">=")) {
                    boolean firsttime = true;
                    boolean firsttimeCell = true;
                    Cell cell = (Cell) deSerialize((String) g.getBuckets().get(gLoc));
                    bucketEntry bE = new bucketEntry(gRecord, "somewhere");
                    int indexB = cell.searchBuckets(bE);
                    Bucket b = cell.getBuckets().get(indexB);
                    int indexBE = b.searchBucketEntry(bE);
                    for(int k=gLoc.get(0); k<10; k++) {
                        Vector<Integer> count = new Vector<>();
                        count.add(k);
                        if(g.getBuckets().containsKey(count)) {
                            cell = (Cell) deSerialize((String) g.getBuckets().get(count));
                            for (int i = 0; i < cell.getBuckets().size(); i++) {
                                if(firsttimeCell){
                                    firsttimeCell = false;
                                    i= indexB;
                                }
                                Bucket bb = cell.getBuckets().get(i);
                                for (int j = 0; j < bb.getEntries().size(); j++) {
                                    if (firsttime) {
                                        firsttime = false;
                                        j = indexBE;
                                    }
                                    bucketEntry bee = bb.getEntries().get(j);
                                    if (bee.getRow() != null) {
                                        BEs.add(bee);
                                    }
                                }
                            }
                        }
                    }
                    return  BEs;
                } else if (operator.equals("<")) {
                    Cell cell = (Cell) deSerialize((String) g.getBuckets().get(gLoc));
                    bucketEntry bE = new bucketEntry(gRecord, "somewhere");
                    int indexB = cell.searchBuckets(bE);
                    Bucket b = cell.getBuckets().get(indexB);
                    int indexBE = b.searchBucketEntry(bE);
                    for(int k=0; k<=gLoc.get(0); k++) {
                        Vector<Integer> count = new Vector<>();
                        count.add(k);
                        if(g.getBuckets().containsKey(count)) {
                            cell = (Cell) deSerialize((String) g.getBuckets().get(count));
                            for (int i = 0; i < cell.getBuckets().size(); i++) {
                                Bucket bb = cell.getBuckets().get(i);
                                for (int j = 0; j < bb.getEntries().size(); j++) {
                                    bucketEntry bee = bb.getEntries().get(j);
                                    if (bee.getRow() != null) {
                                        if(bee.getRow().getContent().get(columnName).equals(columnValue)){
                                            return BEs;
                                        }
                                        BEs.add(bee);
                                    }
                                }
                            }
                        }
                    }
                    return  BEs;
                } else if (operator.equals("<=")) {
                    boolean flag = false ;
                    for(int k=0; k<=gLoc.get(0); k++) {
                        Vector<Integer> count = new Vector<>();
                        count.add(k);
                        if(g.getBuckets().containsKey(count)) {
                            Cell cell = (Cell) deSerialize((String) g.getBuckets().get(count));
                            for (int i = 0; i < cell.getBuckets().size(); i++) {
                                Bucket bb = cell.getBuckets().get(i);
                                for (int j = 0; j < bb.getEntries().size(); j++) {
                                    bucketEntry bee = bb.getEntries().get(j);
                                    if (bee.getRow() != null) {
                                        if(bee.getRow().getContent().get(columnName).equals(columnValue) && !flag){
                                            flag = true;
                                        }
                                        else if (flag && !bee.getRow().getContent().get(columnName).equals(columnValue)){
                                            return BEs ;
                                        }
                                        BEs.add(bee);
                                    }
                                }
                            }
                        }
                    }
                    return  BEs;
                } else if (operator.equals("!=")) {
                    for(int k=0; k<10; k++) {
                        Vector<Integer> count = new Vector<>();
                        count.add(k);
                        if(g.getBuckets().containsKey(count)) {
                            Cell cell = (Cell) deSerialize((String) g.getBuckets().get(count));
                            for (int i = 0; i < cell.getBuckets().size(); i++) {
                                Bucket bb = cell.getBuckets().get(i);
                                for (int j = 0; j < bb.getEntries().size(); j++) {
                                    bucketEntry bee = bb.getEntries().get(j);
                                    if (bee.getRow() != null) {
                                        if(!bee.getRow().getContent().get(columnName).equals(columnValue)){
                                            BEs.add(bee);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    return  BEs;
                }
        }


        //check if column is clustering
        if (columnName.equals(table.getClusteringKey())){
            Hashtable<String, Object> data = new Hashtable<>();
            data.put(columnName, columnValue);
            Record r = new Record(data, table.getClusteringKey());
            int indexP = db.get(tableName).searchPage(r);
            Vector curPage = ((Vector)db.get(tableName).getPages().get(indexP));
            Page p = deSerializePage((String)curPage.get(0));
            int[] indexR = p.searchRecord(r);


            if (operator.equals("=")){
                if (indexR[1]==0){
                    return BEs ;
                }
                else {
                    bucketEntry bE = new bucketEntry((Record)p.getTuples().get(indexR[0]),(String)curPage.get(0));
                    BEs.add(bE);
                }
                return BEs;
            }
            else if(operator.equals(">=")){
                boolean firsttime = true ;
                for (int i = indexP; i < db.get(tableName).getPages().size();i++){
                    curPage = ((Vector)db.get(tableName).getPages().get(i));
                    p = deSerializePage((String)curPage.get(0));
                    for(int j = 0 ; j < p.getTuples().size();j++){
                        if (firsttime){
                            firsttime = false;
                            j = indexR[0];
                        }
                        if (((Record)p.getTuples().get(j)).compareToValue(columnValue,columnName,(String) table.getColNameType().get(columnName))>=0){
                            bucketEntry bE = new bucketEntry((Record)p.getTuples().get(j),(String)curPage.get(0));
                            BEs.add(bE);
                        }
                    }
                }
                return BEs;
            }
            else if(operator.equals(">")){
                boolean firsttime = true ;
                for (int i = indexP; i < db.get(tableName).getPages().size();i++){
                    curPage = ((Vector)db.get(tableName).getPages().get(i));
                    p = deSerializePage((String)curPage.get(0));
                    for(int j = 0 ; j < p.getTuples().size();j++){
                        if (firsttime){
                            firsttime = false;
                            j = indexR[0];
                        }
                        if (((Record)p.getTuples().get(j)).compareToValue(columnValue,columnName,(String) table.getColNameType().get(columnName))>0){
                            bucketEntry bE = new bucketEntry((Record)p.getTuples().get(j),(String)curPage.get(0));
                            BEs.add(bE);
                        }
                    }
                }
                return BEs;

            }
            else if(operator.equals("<=")){
                for (int i = 0; i <= indexP;i++){
                    curPage = ((Vector)db.get(tableName).getPages().get(i));
                    p = deSerializePage((String)curPage.get(0));
                    for(int j = 0 ; j < p.getTuples().size();j++){
                        if (((Record)p.getTuples().get(j)).compareToValue(columnValue,columnName,(String) table.getColNameType().get(columnName))<=0){
                            bucketEntry bE = new bucketEntry((Record)p.getTuples().get(j),(String)curPage.get(0));
                            BEs.add(bE);
                        }
                        else {
                            return BEs;
                        }
                    }
                }
                return BEs;
            }
            else if(operator.equals("<")){
                for (int i = 0; i <=indexP;i++){
                    curPage = ((Vector)db.get(tableName).getPages().get(i));
                    p = deSerializePage((String)curPage.get(0));
                    for(int j = 0 ; j < p.getTuples().size();j++){
                        if (((Record)p.getTuples().get(j)).compareToValue(columnValue,columnName,(String) table.getColNameType().get(columnName))<0){
                            bucketEntry bE = new bucketEntry((Record)p.getTuples().get(j),(String)curPage.get(0));
                            BEs.add(bE);
                        }
                        else {
                            return BEs;
                        }
                    }
                }
                return BEs;

            }
            else if(operator.equals("!=")){
                for (int i = 0; i < db.get(tableName).getPages().size();i++){
                    curPage = ((Vector)db.get(tableName).getPages().get(i));
                    p = deSerializePage((String)curPage.get(0));
                    for(int j = 0 ; j < p.getTuples().size();j++){
                        if (((Record)p.getTuples().get(j)).compareToValue(columnValue,columnName,(String) table.getColNameType().get(columnName))!=0){
                            bucketEntry bE = new bucketEntry((Record)p.getTuples().get(j),(String)curPage.get(0));
                            BEs.add(bE);
                        }
                    }
                }
                return BEs;
            }
        }// end of IF clusteringKey

        // if not then brute force :)
        else{
            for (int i = 0; i < db.get(tableName).getPages().size();i++){
                Vector curPage = ((Vector)db.get(tableName).getPages().get(i));
                Page p = deSerializePage((String)curPage.get(0));
                for(int j = 0 ; j < p.getTuples().size();j++){
                    if (operator.equals("=")){
                        if (((Record)p.getTuples().get(j)).compareToValue(columnValue,columnName,(String) table.getColNameType().get(columnName))==0){
                            bucketEntry bE = new bucketEntry((Record)p.getTuples().get(j),(String)curPage.get(0));
                            BEs.add(bE);
                        }
                    }
                    else if(operator.equals(">=")) {
                        if (((Record)p.getTuples().get(j)).compareToValue(columnValue,columnName,(String) table.getColNameType().get(columnName))>=0){
                            bucketEntry bE = new bucketEntry((Record)p.getTuples().get(j),(String)curPage.get(0));
                            BEs.add(bE);
                        }
                    }
                    else if(operator.equals(">")) {
                        if (((Record)p.getTuples().get(j)).compareToValue(columnValue,columnName,(String) table.getColNameType().get(columnName))>0){
                            bucketEntry bE = new bucketEntry((Record)p.getTuples().get(j),(String)curPage.get(0));
                            BEs.add(bE);
                        }
                    }
                    else if(operator.equals("<=")) {
                        if (((Record)p.getTuples().get(j)).compareToValue(columnValue,columnName,(String) table.getColNameType().get(columnName))<=0){
                            bucketEntry bE = new bucketEntry((Record)p.getTuples().get(j),(String)curPage.get(0));
                            BEs.add(bE);
                        }
                    }
                    else if(operator.equals("<")) {
                        if (((Record)p.getTuples().get(j)).compareToValue(columnValue,columnName,(String) table.getColNameType().get(columnName))<0){
                            bucketEntry bE = new bucketEntry((Record)p.getTuples().get(j),(String)curPage.get(0));
                            BEs.add(bE);
                        }
                    }
                    else if(operator.equals("!=")) {
                        if (((Record)p.getTuples().get(j)).compareToValue(columnValue,columnName,(String) table.getColNameType().get(columnName))!=0){
                            bucketEntry bE = new bucketEntry((Record)p.getTuples().get(j),(String)curPage.get(0));
                            BEs.add(bE);
                        }
                    }

                }
            }
            return BEs;
        }
        return  BEs ;
    }

    public static Vector<bucketEntry> queryAND(Vector<bucketEntry> op1, Vector<bucketEntry> op2){
        Set<bucketEntry> set1 = new HashSet<bucketEntry>(op1);
        Set<bucketEntry> set2 = new HashSet<bucketEntry>(op2);
        set1.stream().filter(set2::contains).collect(Collectors.toSet());
        Vector<bucketEntry> res = new Vector<>();
        res.addAll(set1);
        return res;
    }

    public static Vector<bucketEntry> queryOR(Vector<bucketEntry> op1, Vector<bucketEntry> op2){
        Set<bucketEntry> set1 = new HashSet<bucketEntry>(op1);
        Set<bucketEntry> set2 = new HashSet<bucketEntry>(op2);
        set1.addAll(set2);
        Vector<bucketEntry> res = new Vector<>();
        res.addAll(set1);
        return res;
    }

    public static Vector<bucketEntry> queryXOR(Vector<bucketEntry> op1, Vector<bucketEntry> op2){
        Vector<bucketEntry> b = queryOR(op1,op2);
        b.removeAll(queryAND(op1,op2));
        return b;
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
        }
        catch(IOException ex)
        {
            ex.printStackTrace();
        }
    }

    public void serialize(Object object, String filename){
        try
        {
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/data/"+filename + ".ser");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(object);

            out.close();
            file.close();
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
    public static Object deSerialize(String filename){
        Object output = null;

        try
        {
            // Reading the object from a file
            FileInputStream file = new FileInputStream("src/main/resources/data/"+filename+".ser");
            ObjectInputStream in = new ObjectInputStream(file);

            // Method for deserialization of object
            output = in.readObject();

            in.close();
            file.close();
        }

        catch(IOException ex)
        {
            System.out.println("IOException is caught of deserialze line 1168");
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


    public void updateCsv(String tableName,String[] columnNames) {
        List<String> columns = new Vector<>(Arrays.asList(columnNames));
        try {
            File inputFile = new File("src/main/resources/metadata.csv");

            // Read existing file
            CSVReader reader = new CSVReader(new FileReader(inputFile));
            List<String[]> csvBody = reader.readAll();
            System.out.println(csvBody);

            // check and update
            for(String[] line : csvBody){
                if(line[0].equals(tableName)){
                    if(columns.contains(line[1])){
                        line[4] = "TRUE";
                    }
                }
            }
            reader.close();

            // Write to CSV file which is open
            CSVWriter writer = new CSVWriter(new FileWriter(inputFile),
                    CSVWriter.DEFAULT_SEPARATOR,
                    CSVWriter.NO_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                    CSVWriter.RFC4180_LINE_END);
            writer.writeAll(csvBody);
            writer.flush();
            writer.close();
        }catch(Exception e){
            System.out.println("error occurred while updating index");
        }
    }

}
