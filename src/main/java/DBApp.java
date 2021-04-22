import java.io.*;
import java.util.*;

public class DBApp implements DBAppInterface{

    private HashMap<String,DBTable> db= new HashMap();


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
            if(getFileSize()>0){
            db = getMap("src/main/resources/metadata.csv");
//            db.get("CityShop").displayAttributes();
//            db.get("notCityShop").displayAttributes();
            } else {
                FileWriter fr = new FileWriter("src/main/resources/metadata.csv");
                BufferedWriter br2 = new BufferedWriter(fr);
                String d = "Table Name, Column Name, Column Type, ClusteringKey, Indexed , min , max"+"\n";
                br2.write(d);
                br2.close();
                fr.close();
               System.out.println("gowa el else");
            }

        } catch (IOException i) {
            i.printStackTrace();
            System.out.println("NO PREVIOUS DB!!!");
            return;
        }



    }

    public static int getFileSize() throws IOException {
       int i= 0;
        try{
            FileReader fr = new FileReader("src/main/resources/metadata.csv");
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
        //uptade the csv


        if(clusteringKey==null)
            throw new NoClusterException("TABLE MUST HAVE A CLUSTERING KEY!!!");
        if(tableName==null)
            throw new NoTableNameException("TABLE MUST HAVE A NAME!!!");

        for(Map.Entry m: colNameType.entrySet()){
            if(!(m.getValue().equals("java.lang.Integer")) && !(m.getValue().equals("java.lang.String")) && !(m.getValue().equals("java.lang.Double")) && !(m.getValue().equals("java.util.Date")) ) {
                throw new InvalidDataTypeException(m.getValue()+ " IS NOT A VALID TYPE!!!");
            }
            if(m.getKey().equals(clusteringKey))
                hasCluster=true;
        }
        if(!hasCluster)
            throw new NoClusterException("TABLE MUST HAVE A CLUSTERING KEY!!!");
        if(colNameType.size()!=colNameMin.size() || colNameType.size()!=colNameMax.size() || colNameMin.size()!=colNameMax.size())
            throw new tableMismatchException("Sizes of columns are incompatible");

        for(Map.Entry m: colNameType.entrySet()){
            if(!colNameMin.containsKey(m.getKey()) || !colNameMax.containsKey(m.getKey()))
                throw new tableMismatchException("Table content mismatch");
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
                throw new InvalidDataTypeException("Attribute must be unique") ;

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

    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        //init a page on first insert
        // Object violating min/max constraints              5
        // duplicate cluster key   (AFTER CREATING RECORDS)  3
        // Col name doesn't exist    (CHECK)      2
        // table name doesn't exist/null  (CHECK) 1
        // Object matches type                    4
        // N constraint   to be read from the config
        // No primary key   (CHECK)               3

        if(tableName==null)
            throw new NoTableNameException("TABLE MUST HAVE A NAME!!!");
        if(!db.containsKey(tableName))
            throw new NoTableNameException("TABLE NAME NOT FOUND");
        if(!colNameValue.containsKey(db.get(tableName).getClusteringKey()))
            throw new NoClusterException("No primary key selected");


//        for(Map.Entry m: colNameValue.entrySet()) {
//            if(!this.db.get(tableName).getColNameType().containsKey(m.getKey()))
//                throw new tableMismatchException("Column not found");
//        }
//
//
//
//        for(Map.Entry m: colNameValue.entrySet()){
//            if(m.getValue() instanceof (Object)(this.db.get(tableName).getColNameType().get(m.getKey())))
//                throw new tableMismatchException("Column mismatch");
//        }

    }

    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {











    }

    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {

    }

    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        return null;
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
