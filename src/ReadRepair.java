import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReadRepair extends Thread{
    List<ProcessData> data;
    Integer averageDelays[];
    Map<Integer, Integer> processToPort;
    
    ReadRepair(List<ProcessData> l, Integer[] averageDelays, Map<Integer, Integer> processToPort){
        this.data = l;
        this.averageDelays = averageDelays;
        this.processToPort = processToPort;
    }
    
    public void run(){
                
        Collections.sort(data);
        Integer val = data.get(0).getData().getValue();
        Long maxTS = data.get(0).getData().getTimestamp();
        // do error handling.
        if(val == -1){
            System.out.println();
        }else{
            System.out.println("Most recent written value = " + val);
        }
        for(int i = 1;i< data.size();i++){
            ProcessData tmp = data.get(i);
            if(maxTS > tmp.getData().getTimestamp()){
                StringBuffer messageBuilder = new StringBuffer();
                messageBuilder.append("read_repair");
                messageBuilder.append(" " + tmp.getKey().toString());
                messageBuilder.append(" " + val.toString());
                messageBuilder.append(" " + maxTS.toString());
                String message = messageBuilder.toString();
                System.out.println("replica id " + tmp.getReplicaId());
                System.out.println("process id " + tmp.getProcessId());
                Sender h  = new Sender(message, averageDelays[tmp.getReplicaId()], processToPort.get(tmp.getProcessId()));
                h.start();
                
            }
        }
        
    }
}