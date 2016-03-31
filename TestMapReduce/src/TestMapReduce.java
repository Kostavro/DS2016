import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

public class TestMapReduce {

	public static void main(String[] args) {
		
		ArrayList<ArrayList<String>> load = splitter(40.78,40.88,-74.9,-73.7,"'2012-04-30 00:00:00'","'2012-05-06 00:00:00'");
		
		
		Thread maper1 = new Maper(load.get(0),"localhost", 4322);
		Thread maper2 = new Maper(load.get(1), "localhost", 4322);
		Thread maper3 = new Maper(load.get(2),"localhost" , 4322);
		maper1.start();
		maper2.start();	
		maper3.start();

		System.out.println(maper1.isAlive());
		System.out.println(maper2.isAlive());
		System.out.println(maper3.isAlive());
		Socket reducer = null;
		ObjectOutputStream rout;
		ObjectInputStream rin;
		
		while(maper1.isAlive() && maper2.isAlive() && maper3.isAlive()){}
		
		System.out.println("Drop it like its hot");
		
		try {
			//reducer = new Socket(InetAddress.getByName("127.0.0.1"), 4323);
			rout = new ObjectOutputStream(reducer.getOutputStream());
			rin   = new ObjectInputStream(reducer.getInputStream());
			
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		
	
	}
	
	private static ArrayList<ArrayList<String>> splitter(double minlat, double maxlat, double minlong, double maxlong, String startTime, String endTime){
		double latpart = Math.abs(maxlat - minlat)/3;
		
		//double difference = 10^(-14);
		System.out.println(latpart);
		
		
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
	private static class Maper extends Thread{
	
		ArrayList<String> task;
		String ip;
		int port;
		
		public Maper(ArrayList<String> task,String ip, int port){
			this.task = task;
			this.ip = ip;
			this.port = port;
			System.out.println(this.getId());
		}
		@Override
		public void run() {
			Socket socket = null;
			ObjectInputStream in = null;
			ObjectOutputStream out = null;
			
			String message;

			try {
				socket = new Socket(InetAddress.getByName(ip), port);
				out = new ObjectOutputStream(socket.getOutputStream());
				in = new ObjectInputStream(socket.getInputStream());

				try {
					message = (String) in.readObject();
					System.out.println("Server>" + message);
					
					out.writeObject(task);
					out.flush();
					String answer = (String) in.readObject();
					System.out.println(answer);
					
					
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



