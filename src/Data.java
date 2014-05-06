
public class Data {
    
    private Integer value;
    private Long timestamp;
    private String lastOperation;
    
    public Data(Integer value, Long timestamp, String lastOperation) {
        this.value = value;
        this.timestamp = timestamp;
        this.lastOperation = lastOperation;
    }

    public int getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getLastOperation() {
        return lastOperation;
    }

    public void setTimestamp(String lastOperation) {
        this.lastOperation = lastOperation;
    }    
}
