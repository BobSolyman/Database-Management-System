import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

public class DBApp implements DBAppInterface{

    static HashMap<String,DBTable> db= new HashMap();


    //We init a DB by giving ColNameType hashtable ex put.("ID",INT) ; put.("Name",String);
    //Max and Min are handled by giving type +MAX/MIN ex Min.put("ID", "0"); Max.put("ID","6969");
    //key is name
    //value is type

    public void init() {
        System.out.println("Good Morning BBE");
    }

    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        boolean hasCluster=false;

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
        if(!this.db.containsKey(tableName))
            throw new NoTableNameException("TABLE NAME NOT FOUND");

        for(Map.Entry m: colNameValue.entrySet()) {
            if(!this.db.get(tableName).getColNameType().containsKey(m.getKey()))
                throw new tableMismatchException("Column not found");
        }

        if(!colNameValue.containsKey(this.db.get(tableName).getClusteringKey()))
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
