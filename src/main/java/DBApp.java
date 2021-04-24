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
            if(getFileSize("src/main/resources/pageLocations.csv")>0){
                readLocation();
            }else{
                FileWriter fr = new FileWriter("src/main/resources/pageLocations.csv");
                BufferedWriter br2 = new BufferedWriter(fr);
                String d = "Location, max, min, tableName, size"+"\n";
                br2.write(d);
                br2.close();
                fr.close();
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
        boolean hasCluster=false;
        //update the csv


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
        //init a page on first insert
        // Object violating min/max constraints              5

        // Object matches type                    4
        // N constraint   to be read from the config

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

<<<<<<< Updated upstream
=======
        for(Map.Entry m: colNameValue.entrySet()){
            String entryType = (String) db.get(tableName).getColNameType().get(m.getKey());
            if(entryType.equals("java.lang.Integer")){
                boolean bound = false;
                try {
                    int currentValue = Integer.parseInt((String)m.getValue());
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
                try {
                    Date currentValue = new SimpleDateFormat("yyyy-MM-dd").parse((String)m.getValue());
                }
                catch (Exception e){
                    throw new DBAppException("Type mismatch: supposed to be a Date.");
                }
            }
            else if(entryType.equals("java.lang.String")){
                boolean bound = false;
                try {
                    String currentValue = (String)m.getValue();
                    String currentMin = (String) db.get(tableName).getColNameMin().get(m.getKey());
                    String currentMax = (String) db.get(tableName).getColNameMax().get(m.getKey());
                    System.out.println(currentMin+"----"+currentMax);
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
>>>>>>> Stashed changes
        //
//        for(Map.Entry m: colNameValue.entrySet()){
//            if(!((Object)m.getValue()).getClass().getName().equals(db.get(tableName).getColNameType().get(m.getKey()))){
//                throw new DBAppException("Columns mismatch");
//            }
//            if(((Object)m.getValue()).getClass().getName().equals("java.lang.String")) {
//                String s = (String)m.getValue();
//                if( s.compareTo(db.get(tableName).getColNameMin().get(m.getKey()))<0 || s.compareTo(db.get(tableName).getColNameMax().get(m.getKey()))>0)
//                    throw new DBAppException("Boundary error");
//            }
//            else if(((Object)m.getValue()).getClass().getName().equals("java.lang.Double")){
//               Double s = (Double)m.getValue();
//                if(s<Double.parseDouble(db.get(tableName).getColNameMin().get(m.getKey())) || s>Double.parseDouble(db.get(tableName).getColNameMax().get(m.getKey())))
//                    throw new DBAppException("Boundary error");
//                }
//            else if(((Object)m.getValue()).getClass().getName().equals("java.lang.Integer")){
//                int s = (int)m.getValue();
//                if(s<Integer.parseInt(db.get(tableName).getColNameMin().get(m.getKey())) || s>Integer.parseInt(db.get(tableName).getColNameMax().get(m.getKey())))
//                    throw new DBAppException("Boundary error");
//            }
//            else if(((Object)m.getValue()).getClass().getName().equals("java.util.Date")){
//                String min = db.get(tableName).getColNameMin().get(m.getKey());
//                Date date1=new SimpleDateFormat("dd/MM/yyyy").parse(min);
//
//                String max = db.get(tableName).getColNameMax().get(m.getKey());
//                Date date2=new SimpleDateFormat("dd/MM/yyyy").parse(max);
//
//                Date s = (Date)m.getValue();
//                if(s.compareTo(date1)<0 || s.compareTo(date2)>0)
//                    throw new DBAppException("Boundary error");
//            }
//            else{
//                throw new DBAppException("Invalid input");
//            }
//
//        }

        //Get the table and find the clustering key
        DBTable curTable= db.get(tableName);
        Object clusteringKey= curTable.getClusteringKey();
        Object curKey= colNameValue.get(clusteringKey);


        //Check if there are no pages in the table
        if(curTable.getPages().size() == 0){
            try {
                Page p = new Page(tableName);
                p.setMax(curKey);
                p.setMin(curKey);
                Record r = new Record(colNameValue,(String) clusteringKey);
                p.insertRecord(r);
                serializePage(p,tableName+"0");
                updateLocation(tableName+"0",p);
                //System.out.println(curTable.getPages());
            }
            catch(IOException e){
                e.printStackTrace();
            }
        }
        else{
            //LOOP AND CHECK TYPE AND LOCATION THEN INSERT AND RETURN, IF NO LOOP ENDED WITHOUT RETURNING THEN NEW PAGE
            for (int i = 0; i < curTable.getPages().size(); i++) {
                Vector curPage= ((Vector)curTable.getPages().get(i));
                Object max= curPage.get(1);
                Object min= curPage.get(2);
                Page p = null;
                if(max instanceof Integer){
                    //check if page has space
                    if(((Integer)curPage.get(4)).compareTo(maxTuples)<0){
                    //value in the middle
                    if(((Integer)max).compareTo((Integer)curKey)>=0 && (((Integer)min).compareTo((Integer)curKey))<=0){
                            p = deSerializePage((String) curPage.get(0));
                            Record r = new Record(colNameValue,(String) clusteringKey);
                            if(p.getTuples().contains(r)){
                                throw new DBAppException("Clustering Key already exists in table");
                            }
                            p.insertRecord(r);
                            serializePage(p,(String) curPage.get(0));
                            updateLocation((String) curPage.get(0),p);
                            return;
                    //value is new max
                    }else  if(((Integer)max).compareTo((Integer)curKey)<0 ){
                        p= deSerializePage((String) curPage.get(0));
                        p.setMax(curKey);
                        Record r = new Record(colNameValue,(String) clusteringKey);
                        p.insertRecord(r);
                        serializePage(p,(String) curPage.get(0));
                        updateLocation((String) curPage.get(0),p);
                        return;
                     //value is new min
                    }else if((((Integer)min).compareTo((Integer)curKey))>0){
                        p= deSerializePage((String) curPage.get(0));
                        p.setMin(curKey);
                        Record r = new Record(colNameValue,(String) clusteringKey);
                        p.insertRecord(r);
                        serializePage(p,(String) curPage.get(0));
                        updateLocation((String) curPage.get(0),p);
                        return;
                    }
                    }
                }
                if(max instanceof Double){
                    //check if page has space
                    if(((Integer)curPage.get(4)).compareTo(maxTuples)<0){
                        //value in the middle
                        if(((Double)max).compareTo((Double)curKey)>=0 && (((Double)min).compareTo((Double)curKey))<=0){
                            p = deSerializePage((String) curPage.get(0));
                            Record r = new Record(colNameValue,(String) clusteringKey);
                            if(p.getTuples().contains(r)){
                                throw new DBAppException("Clustering Key already exists in table");
                            }
                            p.insertRecord(r);
                            serializePage(p,(String) curPage.get(0));
                            updateLocation((String) curPage.get(0),p);
                            return;
                            //value is new max
                        }else  if(((Double)max).compareTo((Double)curKey)<0 ){
                            p= deSerializePage((String) curPage.get(0));
                            p.setMax(curKey);
                            Record r = new Record(colNameValue,(String) clusteringKey);
                            p.insertRecord(r);
                            serializePage(p,(String) curPage.get(0));
                            updateLocation((String) curPage.get(0),p);
                            return;
                            //value is new min
                        }else if((((Double)min).compareTo((Double)curKey))>0){
                            p= deSerializePage((String) curPage.get(0));
                            p.setMin(curKey);
                            Record r = new Record(colNameValue,(String) clusteringKey);
                            p.insertRecord(r);
                            serializePage(p,(String) curPage.get(0));
                            updateLocation((String) curPage.get(0),p);
                            return;
                        }
                    }
                }
                if(max instanceof String){
                    //check if page has space
                    if(((Integer)curPage.get(4)).compareTo(maxTuples)<0){
                        //value in the middle
                        if(((String)max).compareTo((String)curKey)>=0 && (((String)min).compareTo((String)curKey))<=0){
                            p = deSerializePage((String) curPage.get(0));
                            Record r = new Record(colNameValue,(String) clusteringKey);
                            if(p.getTuples().contains(r)){
                                throw new DBAppException("Clustering Key already exists in table");
                            }
                            p.insertRecord(r);
                            serializePage(p,(String) curPage.get(0));
                            updateLocation((String) curPage.get(0),p);
                            return;
                            //value is new max
                        }else  if(((String)max).compareTo((String)curKey)<0 ){
                            p= deSerializePage((String) curPage.get(0));
                            p.setMax(curKey);
                            Record r = new Record(colNameValue,(String) clusteringKey);
                            p.insertRecord(r);
                            serializePage(p,(String) curPage.get(0));
                            updateLocation((String) curPage.get(0),p);
                            return;
                            //value is new min
                        }else if((((String)min).compareTo((String)curKey))>0){
                            p= deSerializePage((String) curPage.get(0));
                            p.setMin(curKey);
                            Record r = new Record(colNameValue,(String) clusteringKey);
                            p.insertRecord(r);
                            serializePage(p,(String) curPage.get(0));
                            updateLocation((String) curPage.get(0),p);
                            return;
                        }
                    }
                }
                if(max instanceof Date){
                    //check if page has space
                    if(((Integer)curPage.get(4)).compareTo(maxTuples)<0){
                        //value in the middle
                        if(((Date)max).compareTo((Date)curKey)>=0 && (((Date)min).compareTo((Date)curKey))<=0){
                            p = deSerializePage((String) curPage.get(0));
                            Record r = new Record(colNameValue,(String) clusteringKey);
                            if(p.getTuples().contains(r)){
                                throw new DBAppException("Clustering Key already exists in table");
                            }
                            p.insertRecord(r);
                            serializePage(p,(String) curPage.get(0));
                            updateLocation((String) curPage.get(0),p);
                            return;
                            //value is new max
                        }else  if(((Date)max).compareTo((Date)curKey)<0 ){
                            p= deSerializePage((String) curPage.get(0));
                            p.setMax(curKey);
                            Record r = new Record(colNameValue,(String) clusteringKey);
                            p.insertRecord(r);
                            serializePage(p,(String) curPage.get(0));
                            updateLocation((String) curPage.get(0),p);
                            return;
                            //value is new min
                        }else if((((Date)min).compareTo((Date)curKey))>0){
                            p= deSerializePage((String) curPage.get(0));
                            p.setMin(curKey);
                            Record r = new Record(colNameValue,(String) clusteringKey);
                            p.insertRecord(r);
                            serializePage(p,(String) curPage.get(0));
                            updateLocation((String) curPage.get(0),p);
                            return;
                        }
                    }
                }
            }
            //PAGES FULL CREATE A NEW ONE
            try {
                Page p = new Page(tableName);
                p.setMax(curKey);
                p.setMin(curKey);
                Record r = new Record(colNameValue,(String) clusteringKey);
                p.insertRecord(r);
                serializePage(p,tableName+curTable.getPages().size());
                updateLocation(tableName+curTable.getPages().size(),p);
                //System.out.println(curTable.getPages());
            }
            catch(IOException e){
                e.printStackTrace();
            }
        }









//        for(Map.Entry m: colNameValue.entrySet()){
//            if(m.getValue() instanceof (Object)(this.db.get(tableName).getColNameType().get(m.getKey())))
//                throw new DBAppException("Column mismatch");
//        }

    }

    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {











    }

    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {

    }

    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        return null;
    }



    public void serializePage(Object object, String filename){
        try
        {
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/pages/"+filename + ".ser");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(object);

            out.close();
            file.close();

            System.out.println("Object has been serialized");
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
            FileInputStream file = new FileInputStream("src/main/resources/pages/"+filename+".ser");
            ObjectInputStream in = new ObjectInputStream(file);

            // Method for deserialization of object
            output = (Page)in.readObject();

            in.close();
            file.close();

            System.out.println("Object has been deserialized ");
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
    public void updateLocation(String location,Page page){
        //Get Table
        DBTable curTable= db.get(page.getTable());
        try {
            FileWriter fr = new FileWriter("src/main/resources/pageLocations.csv", true);
            BufferedWriter br = new BufferedWriter(fr);
            br.write(location+","+page.getMax()+","+page.getMin()+","+ curTable.getName()+","+page.getTuples().size()+"\n");
            br.close();
            fr.close();

            //update vector table in DBTable
            //construct my tuple
            Vector pageInfo= new Vector();
            pageInfo.add(location);
            pageInfo.add(page.getMax());
            pageInfo.add(page.getMin());
            pageInfo.add(curTable.getName());
            pageInfo.add(page.getTuples().size());

            //check if it exists
            if(curTable.getPages().contains(pageInfo)){
               int curLocation= curTable.getPages().lastIndexOf(pageInfo);
               curTable.getPages().add(curLocation,pageInfo);
            }
            else{
                //if it doesn't already exist add it
                curTable.getPages().add(pageInfo);
            }

        }
        catch(IOException ex){
            ex.printStackTrace();
        }
    }

    public  void readLocation() throws IOException {
        FileReader fr = new FileReader("src/main/resources/pageLocations.csv");
        BufferedReader br = new BufferedReader(fr);
        String CurrentLine = "";
        br.readLine();

        while((CurrentLine=br.readLine())!=null){
            String[] line = CurrentLine.split(",");
            String location = line[0].trim();
            Object max = (line[1].trim());
            Object min = (line[2].trim());
            String tableName = line[3].trim();
            int size= Integer.parseInt(line[4].trim());

            //construct the vector
            Vector pageInfo= new Vector();
            pageInfo.add(location);
            pageInfo.add(max);
            pageInfo.add(min);
            pageInfo.add(tableName);
            pageInfo.add(size);

            //add it to proper table
            db.get(tableName).getPages().add(pageInfo);
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








}
