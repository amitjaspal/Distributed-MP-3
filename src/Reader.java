import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.Lock;


public class Reader extends Thread {
    
    Integer processId;
    Integer averageDelays[] = new Integer[3];
    Map<Integer, Data> keyValueStore = new HashMap<Integer, Data>();
    Map<Integer, Integer> processToPort = new HashMap<Integer, Integer>();
    Lock lock;
    Boolean []isValidating;
    Boolean []isKeyPresent;
    CyclicBarrier barrier;
    
    int totalProcesses = 4;
    int replicas = 3;
    String HOST_NAME = "127.0.0.1";
    
    public Reader(Integer processId, Integer[] averageDelays,
            Map<Integer, Data> keyValueStore,
            Boolean []isValidating, Boolean []isKeyPresent,
            Map<Integer, Integer> processToPort, Lock lock, CyclicBarrier barrier) {
        
        this.processId = processId;
        this.averageDelays = averageDelays;
        this.keyValueStore = keyValueStore;
        this.processToPort = processToPort;
        this.lock = lock;
        this.isKeyPresent = isKeyPresent;
        this.isValidating = isValidating;
        this.barrier = barrier;
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
                    
                    // Check if a particular key is present in the system or not.
                    checkIfKeyPresent(tokens);
                    
                    System.out.println("Reader woke up");
                    barrier.reset();
                    if(isKeyPresent[0]){
                        System.out.println(" key already present.");
                        continue;
                    }
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
    
    private void checkIfKeyPresent(String []tokens){
        String message;
        isValidating[0] = true;
        isKeyPresent[0] = false;
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
                int delay = averageDelays[i];
                int portNumber = processToPort.get(node);
                message = messageBuilder.toString() + " " + i + " 9";
                Sender h  = new Sender(message, delay, portNumber);
                h.start();
        }
        try{
            barrier.await();
        }catch(Exception e){
            
        }
   }
}


