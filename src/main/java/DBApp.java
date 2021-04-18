import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

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
        HashMap<String,DBTable>  map;
        try {
            FileInputStream fileIn = new FileInputStream("./data/map.ser"); // get the file
            ObjectInputStream in = new ObjectInputStream(fileIn); // read the file
            map = (HashMap<String,DBTable> ) in.readObject(); //place in map
            in.close();
            fileIn.close();
            //Read Pages
        } catch (IOException i) {
            System.out.println("NO PREVIOUS DB!!!");
            return;
        } catch (ClassNotFoundException c) {
            System.out.println("INCORRECT DATATYPE!!!");
            return;
        }
        db=map;
    }

    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        boolean hasCluster=false;
        //init a page

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


        DBTable current= new DBTable(tableName,clusteringKey);
    }

    public void createIndex(String tableName, String[] columnNames) throws DBAppException {
        //Milestone 2
    }

    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        // Object violating min/max constraints
        // duplicate cluster key   (AFTER CREATING RECORDS)
        // Col name doesn't exist    (CHECK)
        // table name doesn't exist/null  (CHECK)
        // Object matches type
        // N constraint
        // No primary key   (CHECK)

        if(tableName==null)
            throw new NoTableNameException("TABLE MUST HAVE A NAME!!!");
        if(!db.containsKey(tableName))
            throw new NoTableNameException("TABLE NAME NOT FOUND");

//        for(Map.Entry m: colNameValue.entrySet()) {
//            if(!this.db.get(tableName).getColNameType().containsKey(m.getKey()))
//                throw new tableMismatchException("Column not found");
//        }

        if(!colNameValue.containsKey(db.get(tableName).getClusteringKey()))
            throw new NoClusterException("No primary key selected");

       // for(Map.Entry m: colNameValue.entrySet()){
        //    if(m.getValue() instanceof (Object)(this.db.get(tableName).getColNameType().get(m.getKey())))
         //       throw new tableMismatchException("Column mismatch");
     //   }

    }

    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {

    }

    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {

    }

    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        return null;
    }

}
