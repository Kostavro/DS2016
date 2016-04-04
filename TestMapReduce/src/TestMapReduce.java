import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestMapReduce {

	public static void main(String[] args) {
		
		ArrayList<ArrayList<String>> load = splitter(40.78,40.88,-74.9,-73.7,"'2012-04-30 00:00:00'","'2012-05-06 00:00:00'");
		
		
		Thread mapper1 = new Mapper(load.get(0), "localhost", 4322);
		Thread mapper2 = new Mapper(load.get(1), "localhost", 4322);
		Thread mapper3 = new Mapper(load.get(2), "localhost", 4322);
		mapper1.start();
		mapper2.start();	
		mapper3.start();

		Socket reducer = null;
		ObjectOutputStream rout;
		ObjectInputStream rin;
		
		while(mapper1.isAlive() && mapper2.isAlive() && mapper3.isAlive()){}
		
		
		 try {
			TimeUnit.SECONDS.sleep(1);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		System.out.println("All done");
		
		try {
			reducer = new Socket(InetAddress.getByName("127.0.0.1"), 4323);
			rout = new ObjectOutputStream(reducer.getOutputStream());
			rin   = new ObjectInputStream(reducer.getInputStream());
			
			rout.writeObject("Reduce");
			rout.flush();
			try {
				Map<String, Long>  results = (Map<String, Long>) rin.readObject();
				System.out.println(Arrays.toString(results.entrySet().toArray()));
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			rout.close();
			rin.close();
			reducer.close();
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}

		
		
	
	}
	
	private static ArrayList<ArrayList<String>> splitter(double minlat, double maxlat, double minlong, double maxlong, String startTime, String endTime){
		double latpart = Math.abs(maxlat - minlat)/3;
		
		ArrayList<String> order1= new ArrayList<String>();
		ArrayList<String> order2= new ArrayList<String>();
		ArrayList<String> order3= new ArrayList<String>();
		
		order1.add(Double.toString(minlat));
		order1.add(Double.toString(minlat+latpart));
		order1.add(Double.toString(minlong));
		order1.add(Double.toString(maxlong));
		order1.add(startTime);
		order1.add(endTime);
		
		order2.add(Double.toString(minlat+latpart));
		order2.add(Double.toString(minlat+2*latpart));
		order2.add(Double.toString(minlong));
		order2.add(Double.toString(maxlong));
		order2.add(startTime);
		order2.add(endTime);  
		
		order3.add(Double.toString(minlat+2*latpart));
		order3.add(Double.toString(maxlat));
		order3.add(Double.toString(minlong));
		order3.add(Double.toString(maxlong));
		order3.add(startTime);
		order3.add(endTime);  
		
		ArrayList<ArrayList<String>> load = new ArrayList<ArrayList<String>>();
		load.add(order1);
		load.add(order2);
		load.add(order3);
		return load;
	}
	private static class Mapper extends Thread{
	
		ArrayList<String> task;
		String ip;
		int port;
		
		public Mapper(ArrayList<String> task,String ip, int port){
			this.task = task;
			this.ip = ip;
			this.port = port;
		}
		@Override
		public void run() {
			Socket socket = null;
			ObjectInputStream in = null;
			ObjectOutputStream out = null;
			
			

			try {
				socket = new Socket(InetAddress.getByName(ip), port);
				out = new ObjectOutputStream(socket.getOutputStream());
				in = new ObjectInputStream(socket.getInputStream());

				try {
				
					
					out.writeObject(task);
					out.flush();
					String mes = (String) in.readObject();
					System.out.println(mes);
					
					
				} catch (ClassNotFoundException e1) {
					System.err.println("Data Received to Unknown Format");
				}
			} catch (UnknownHostException e2) {
				System.err.println("You are trying to connect to an unknown host");

			} catch (IOException e3) {
				e3.printStackTrace();
			} finally {
				try {
					socket.close();
					in.close();
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}
}



