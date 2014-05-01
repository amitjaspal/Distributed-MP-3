import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;


public class Reader extends Thread {
    
    Integer processId;
    Integer averageDelays[] = new Integer[3];
    Map<Integer, Data> keyValueStore = new HashMap<Integer, Data>();
    Map<Integer, Integer> processToPort = new HashMap<Integer, Integer>();
    Lock lock;
    
    int totalProcesses = 4;
    int replicas = 3;
    String HOST_NAME = "127.0.0.1";
    
    public Reader(Integer processId, Integer[] averageDelays,
            Map<Integer, Data> keyValueStore,
            Map<Integer, Integer> processToPort, Lock lock) {
        
        this.processId = processId;
        this.averageDelays = averageDelays;
        this.keyValueStore = keyValueStore;
        this.processToPort = processToPort;
        this.lock = lock;
    }

    public void run() {
        System.out.println("SENDER STARTED !!");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String input;
        String message;
        try{
            while((input = br.readLine()) != null){
                String tokens[] = input.split(" ");
                
                // message format : insert_background key value ts pid replicaId level
                if(tokens[0].toLowerCase().equals("insert")){
                    StringBuffer messageBuilder = new StringBuffer();
                    messageBuilder.append("insert_background");
                    Integer key = Integer.parseInt(tokens[1]);
                    messageBuilder.append(" " + key.toString());
                    Integer value = Integer.parseInt(tokens[2]);
                    messageBuilder.append(" " + value.toString());
                    Long unixTime = System.currentTimeMillis() / 1000L;
                    messageBuilder.append(" " + unixTime.toString());
                    messageBuilder.append(" " + processId.toString());
                    Integer level = Integer.parseInt(tokens[3]);
                    
                    for(int i = 0;i<replicas;i++){
                            int node = (key +i) % totalProcesses;
                            System.out.println("node = " + node);
                            int delay = averageDelays[i];
                            int portNumber = processToPort.get(node);
                            message = messageBuilder.toString() + " " + i + " " + level; 
                            Sender h  = new Sender(message, delay, portNumber);
                            h.start();
                    }
                }
                
                // message format : get_background key ts pid replicaId level
                if(tokens[0].toLowerCase().equals("get")){
                    StringBuffer messageBuilder = new StringBuffer();
                    messageBuilder.append("get_background");
                    Integer key = Integer.parseInt(tokens[1]);
                    messageBuilder.append(" " + key.toString());
                    Long unixTime = System.currentTimeMillis() / 1000L;
                    messageBuilder.append(" " + unixTime.toString());
                    messageBuilder.append(" " + processId.toString());
                    Integer level = Integer.parseInt(tokens[2]);
                    for(int i = 0;i<replicas;i++){
                            int node = (key +i) % totalProcesses;
                            System.out.println("node = " + node);
                            int delay = averageDelays[i];
                            int portNumber = processToPort.get(node);
                            message = messageBuilder.toString() + " " + i + " " + level.toString();
                            Sender h  = new Sender(message, delay, portNumber);
                            h.start();
                        }
                    }   
            }
        }catch( IOException e){
            
        }
    }
}


