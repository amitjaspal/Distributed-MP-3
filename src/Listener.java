import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;


public class Listener extends Thread{

    Integer processId;
    Integer averageDelays[] = new Integer[3];
    Map<Integer, Data> keyValueStore = new HashMap<Integer, Data>();
    Map<Integer, Integer> processToPort = new HashMap<Integer, Integer>();
    Map<String, List<Data> > getRepliesMap= new HashMap<String, List<Data>>();
    Map<String, Integer > insertRepliesMap= new HashMap<String, Integer>();
    
    Lock lock;
    
    public Listener(Integer processId, Integer[] averageDelays,
            Map<Integer, Data> keyValueStore,
            Map<Integer, Integer> processToPort, Lock lock) {
        
        this.processId = processId;
        this.averageDelays = averageDelays;
        this.keyValueStore = keyValueStore;
        this.processToPort = processToPort;
        this.lock = lock;
    }
    
    public void run(){
        System.out.println("LISTNER STARTED !!");
        
        try {
            ServerSocket serverSocket = new ServerSocket(processToPort.get(processId));
            
            InputStream input = null;
            while (true) {
                Socket connection = serverSocket.accept();
                input = connection.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(input));
                String msg = reader.readLine();
                System.out.println("Recieved message - " + msg);
                String tokens[] = msg.split(" ");
                
                // handle inserts 
                if(tokens[0].toLowerCase().equals("insert_background")){
                    Integer key = Integer.parseInt(tokens[1]);
                    Integer value = Integer.parseInt(tokens[2]);
                    Long timestamp = Long.parseLong(tokens[3]);
                    Integer fromProcess = Integer.parseInt(tokens[4]);
                    Integer replicaId = Integer.parseInt(tokens[5]);
                    Integer level = Integer.parseInt(tokens[6]);
                    Data d = new Data(value, timestamp);
                    
                    // inspect the keyValueStore
                    while(!lock.tryLock());
                    if(keyValueStore.containsKey(key)){
                        Data tmp = keyValueStore.get(key);
                        if(tmp.getTimestamp() < timestamp){
                            keyValueStore.put(key, new Data(value, timestamp));
                        }
                    }else{
                        keyValueStore.put(key, new Data(value, timestamp));
                    }
                    lock.unlock();
                    
                    StringBuffer messageBuilder = new StringBuffer();
                    messageBuilder.append("insert_reply");
                    messageBuilder.append(" " + key.toString());
                    messageBuilder.append(" " + timestamp.toString());
                    messageBuilder.append(" " + value.toString());
                    messageBuilder.append(" " + processId.toString());
                    messageBuilder.append(" " + replicaId.toString());
                    messageBuilder.append(" " + level.toString());
                    String message = messageBuilder.toString();
                    Sender h  = new Sender(message, 0, processToPort.get(fromProcess));
                    h.start();
                    
                }
                
                if(tokens[0].toLowerCase().equals("insert_reply")){
                    Integer key = Integer.parseInt(tokens[1]);
                    Long requestTS = Long.parseLong(tokens[2]); 
                    Integer value = Integer.parseInt(tokens[3]);
                    Integer fromProcess = Integer.parseInt(tokens[4]); 
                    Integer replicaId = Integer.parseInt(tokens[5]); 
                    Integer level = Integer.parseInt(tokens[6]); 
                    String mapKey = key.toString() + ":" + requestTS.toString();

                    if(insertRepliesMap.containsKey(mapKey)){
                        int count = insertRepliesMap.get(mapKey);
                        insertRepliesMap.put(mapKey, count + 1);
                        // check for size and perform consistency clean-ups
                        if(count + 1 == 3 && level == 9){
                            System.out.println("Inserted Key with level 9");
                        }
                    }else{
                        insertRepliesMap.put(mapKey, 1);
                        if(level == 1){
                            System.out.println("Inserted Key with level 1");
                        }   
                    }
                }
                
                
                // message format : get_reply key ts value pid replicaId level
                if(tokens[0].toLowerCase().equals("get_background")){
                    Integer key = Integer.parseInt(tokens[1]);
                    Long requestTS = Long.parseLong(tokens[2]); 
                    Integer fromProcess = Integer.parseInt(tokens[3]);
                    Integer replicaId = Integer.parseInt(tokens[4]);
                    Integer level = Integer.parseInt(tokens[5]);
                    
                    while(!lock.tryLock());
                    Integer value = -1;
                    Long timestamp = -1L;
                    if(keyValueStore.containsKey(key)){
                        Data tmp = keyValueStore.get(key);
                        value = tmp.getValue();
                        timestamp = tmp.getTimestamp();
                    }
                    lock.unlock();
                    
                    StringBuffer messageBuilder = new StringBuffer();
                    messageBuilder.append("get_reply");
                    messageBuilder.append(" " + key.toString());
                    messageBuilder.append(" " + tokens[2]);
                    messageBuilder.append(" " + value.toString() + ":" + timestamp.toString());
                    messageBuilder.append(" " + processId.toString());
                    messageBuilder.append(" " + replicaId.toString());
                    messageBuilder.append(" " + level.toString());
                    String message = messageBuilder.toString();
                    Sender h  = new Sender(message, 0, processToPort.get(fromProcess));
                    h.start();
                    
                }
                
                if(tokens[0].toLowerCase().equals("get_reply")){
                    Integer key = Integer.parseInt(tokens[1]);
                    Long requestTS = Long.parseLong(tokens[2]); 
                    Integer value = Integer.parseInt(tokens[3].split(":")[0]);
                    Long valueTS = Long.parseLong(tokens[3].split(":")[1]);
                    Integer fromProcess = Integer.parseInt(tokens[4]); 
                    Integer replicaId = Integer.parseInt(tokens[5]); 
                    Integer level = Integer.parseInt(tokens[6]); 
                    Data tmp = new Data(value, valueTS);
                    tmp.setProcessId(fromProcess);
                    String mapKey = key.toString() + ":" + requestTS.toString();
                    if(getRepliesMap.containsKey(mapKey)){
                        List<Data> l = getRepliesMap.get(mapKey);
                        l.add(tmp);
                        // check for size and perform consistency clean-ups
                        if(l.size() == 3){
                            new ReadRepair(l).start();  
                        }
                    }else{
                        List<Data> d = new ArrayList<Data>();
                        d.add(tmp);
                        getRepliesMap.put(mapKey, d);
                        if(level == 1){
                            System.out.println("Value for KEY " + key + " : " + tmp.getValue());
                        }   
                    }
                    
                    
                    
                }
                
            }
        }catch(Exception e){
            
        }
    }
    
    
}


class ReadRepair extends Thread{
    List<Data> data;
    ReadRepair(List<Data> l){
        this.data = l;
    }
    public void run(){
        Long maxTS = -1L;
        int val = -1;
        for(int i = 0;i< data.size();i++){
            if(maxTS < data.get(i).getTimestamp()){
                val = data.get(i).getValue();
                maxTS = data.get(i).getTimestamp();
            }
        }
        System.out.println("Most recent written value = " + val);
    }
}