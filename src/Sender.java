import java.io.PrintWriter;
import java.net.Socket;

class Sender extends Thread 
{
    private String message;
    private Integer delay;
    private Integer portNumber; 
    String HOST_NAME = "127.0.0.1";
    
    Sender( String message, Integer delay, Integer portNumber){
        this.message = message;
        this.delay = delay;
        this.portNumber = portNumber;
    }
    
    public void run(){
        
        
        try {
               //Introducing a random delay in the range 0, 2 * delay
               Thread.sleep((long) Math.ceil((Math.random() * delay * 2)));
               
               Socket socket = new Socket(HOST_NAME, portNumber);
               PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
               out.println(message);
               out.flush();
               System.out.println("port number - " + portNumber);
               System.out.println("Message sent " + message);
               socket.close();
            } catch (Exception e) {
    
            }   
        
    }
}