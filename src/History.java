import java.util.List;

public class History {
	Long timestamp;
	String operation;

	public History(Long timestamp, String operation) {
		this.timestamp = timestamp;
		this.operation = operation;
	}
    
    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    } 

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}