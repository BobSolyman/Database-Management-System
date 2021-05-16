public class bucketEntry {
    private  Record row ;
    private  String pageLoc ;

    public bucketEntry(Record row, String pageLoc) {
        this.row = row;
        this.pageLoc = pageLoc;
    }

    public Record getRow() {
        return row;
    }

    public void setRow(Record row) {
        this.row = row;
    }

    public String getPageLoc() {
        return pageLoc;
    }

    public void setPageLoc(String pageLoc) {
        this.pageLoc = pageLoc;
    }
}
