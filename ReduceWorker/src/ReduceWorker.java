import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class ReduceWorker {
	
	private static int srvPort = 4323;

	
	public static void main(String[] args) {
		startServer();
	}
	
	private static void startServer() {
		ServerSocket socket = null;
		Socket connection = null;
		Map<String, Long> mapped = new HashMap<String, Long>();
		ReentrantLock lock = new ReentrantLock();
		try {
			socket = new ServerSocket(srvPort);

			while (true) {
				
				connection = socket.accept();
				Thread t = new Reducer(connection,mapped,lock);
				t.start();
				
				
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			try {
				socket.close();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
	}	
}

class Reducer extends Thread{
	ObjectOutputStream out;
	ObjectInputStream in ;
	Socket client;
	Map<String, Long> mapped;
	ReentrantLock lock;
	
	public Reducer(Socket connection, Map<String, Long> mapped, ReentrantLock lock){     
		this.client = connection;
		this.mapped = mapped;
		this.lock = lock;
		
		try {
			out = new ObjectOutputStream(connection.getOutputStream());
			in = new ObjectInputStream(connection.getInputStream());		
		} catch (IOException e){
			e.printStackTrace();
		}
		
	}
	@Override
	
	public void run() {
		try {
			try {
					lock.lock();
					try {
						try {
							mapped.putAll( (Map<String, Long>) in.readObject());
						
						} catch (ClassCastException e){
							out.writeObject(reduce(mapped,4));
							out.flush();
							mapped.clear();
						}
					}
					finally{
						lock.unlock();
					}
		
			
			
		} catch (ClassNotFoundException e) {
			
			e.printStackTrace();
		}	
		
		in.close();
		out.close();
		client.close();
		} catch (IOException e){
			System.out.println("Sam ting wong");
		}
	}
	
private static Map<String, Long> reduce(Map<String, Long> load, int topk){
		
		Map<String, Long> results = ((Map<String, Long>) load).entrySet().stream()
				.sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
				.limit(topk)
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));;
		
		return results;
	}
}
