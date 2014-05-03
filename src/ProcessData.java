
public class ProcessData implements Comparable<ProcessData>{
    
    private Data data;
    private Integer processId;
    private Integer replicaId;
    private Integer key;
    
    
    
    public Data getData() {
        return data;
    }



    public void setData(Data data) {
        this.data = data;
    }



    public Integer getProcessId() {
        return processId;
    }



    public void setProcessId(Integer processId) {
        this.processId = processId;
    }



    public Integer getReplicaId() {
        return replicaId;
    }



    public void setReplicaId(Integer replicaId) {
        this.replicaId = replicaId;
    }



    public Integer getKey() {
        return key;
    }



    public void setKey(Integer key) {
        this.key = key;
    }



    @Override
    public int compareTo(ProcessData p) {
        Long ts1 = p.getData().getTimestamp();
        return ts1.compareTo(data.getTimestamp());
    }
    
}
