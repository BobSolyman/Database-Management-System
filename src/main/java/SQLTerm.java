import java.util.Iterator;

public class SQLTerm{
    public String _strTableName;
    public String _strColumnName;
    public String _strOperator;
    public Object _objValue;

    public String get_strTableName() {
        return _strTableName;
    }


    public String get_strColumnName() {
        return _strColumnName;
    }

    public void set_strColumnName(String _strColumnName) {
        this._strColumnName = _strColumnName;
    }

    public String get_strOperator() {
        return _strOperator;
    }

    public void set_strOperator(String _strOperator) {
        this._strOperator = _strOperator;
    }

    public Object get_objValue() {
        return _objValue;
    }

    public void set_objValue(Object _objValue) {
        this._objValue = _objValue;
    }
}
