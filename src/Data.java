
public class Data {
    
    private int value;
    private long timestamp;
    private int processId;
    private int replicaId;
    
    
    public int getReplicaId() {
        return replicaId;
    }

    public void setReplicaId(int replicaId) {
        this.replicaId = replicaId;
    }

    public Data(int value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getProcessId() {
        return processId;
    }

    public void setProcessId(int processId) {
        this.processId = processId;
    }
    
}
