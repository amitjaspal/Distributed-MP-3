import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


class Initializer {
    Integer processId;
    Integer averageDelays[] = new Integer[3];
    Map<Integer, Data> keyValueStore = new HashMap<Integer, Data>();
    Map<Integer, Integer> processToPort = new HashMap<Integer, Integer>();
    Lock lock;
    String configFileName;
    
    public void start(String []args) throws IOException{
        
        processId = Integer.parseInt(args[0]);
        configFileName = args[1];
        averageDelays[0] = Integer.parseInt(args[2]);
        averageDelays[1] = Integer.parseInt(args[3]);
        averageDelays[2] = Integer.parseInt(args[4]);
        lock = new ReentrantLock();
        CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
        
        Boolean []isValidating = new Boolean [] {new Boolean(false)};
        Boolean []isKeyPresent = new Boolean [] {new Boolean(false)};
        
        String str;
        BufferedReader brFile = new BufferedReader(new FileReader(configFileName));
        while ((str = brFile.readLine()) != null) {
            processToPort.put(Integer.parseInt(str.split(" ")[0]), Integer.parseInt(str.split(" ")[1]));
        }
        
        brFile.close();
        
        Listener l = new Listener(processId, averageDelays, keyValueStore, isValidating, isKeyPresent, processToPort, lock, cyclicBarrier);
        l.start();
        
        Reader s = new Reader(processId, averageDelays, keyValueStore, isValidating, isKeyPresent, processToPort, lock, cyclicBarrier);
        s.start();
        
        
    }
    
}
