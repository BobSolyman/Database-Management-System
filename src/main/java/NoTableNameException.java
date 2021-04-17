public class NoTableNameException extends DBAppException{
    public NoTableNameException(){
        super();
    }
    public NoTableNameException(String message){
        super(message);
    }
}