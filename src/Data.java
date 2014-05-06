import java.util.ArrayList;
import java.util.List;


public class Data {
    
    private Integer value;
    private Long timestamp;
    private String operation;
    private List<History> operationHistory;
    
    public Data(Integer value, Long timestamp, String operation) {
        this.value = value;
        this.timestamp = timestamp;
        this.operation = operation;
        operationHistory = new ArrayList<History>();
        operationHistory.add(new History(timestamp, operation));
    }

    public List<History> getHistory()
    {
    	return operationHistory;
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
    
    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    } 
}
