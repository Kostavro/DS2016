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
import java.util.stream.Collectors;

public class ReduceWorker {
	
	static int srvPort = 4323;
	static Map<String, Long> mapped;
	
	public static void main(String[] args) {
		mapped = new HashMap<String, Long>();
		startServer();
	}
	
	public static void startServer() {
		ServerSocket socket = null;
		Socket connection = null;

		try {
			socket = new ServerSocket(srvPort);

			while (true) {
				
				connection = socket.accept();
				Thread t = new Client(connection);
				t.start();
				
				
			}
			
		} catch (IOException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		finally {
			try {
				System.out.println("im done");
				socket.close();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
	}	
	
	public static Map<String, Long> reduce(Map<String, Long> load, int topk){
		
		Map<String, Long> results = (Map<String, Long>) load.entrySet().parallelStream()
				.sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
				.limit(topk)
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));;
		
		return results;
	}
	
	
	static class Client extends Thread{
		ObjectOutputStream out;
		ObjectInputStream in ;
		Socket client;

		
		public Client(Socket connection){     
			this.client = connection;
			
			
			try {
				out = new ObjectOutputStream(connection.getOutputStream());
				in = new ObjectInputStream(connection.getInputStream());		
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		@Override
		
		public void run() {
			
			
			try{
				
				try{
					try{
					mapped.putAll((Map<String, Long>) in.readObject());
					//System.out.println(Arrays.toString(mapped.entrySet().toArray()));
					}catch(ClassCastException e)
					{
					out.writeObject(reduce(mapped,4));
					out.flush();
					}
					
				
				
			} catch (ClassNotFoundException e) {
				
				e.printStackTrace();
			}	
			
			in.close();
			out.close();
			client.close();
			}catch(IOException e){
				System.out.println("Sam ting wong");
			}
		}
	}
}
