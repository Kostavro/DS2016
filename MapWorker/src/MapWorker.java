import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MapWorker {
	private static String dbURL = "jdbc:mysql://83.212.117.76:3306/ds_systems_2016?user=omada66&password=omada66db";
	private static String dbClass = "com.mysql.jdbc.Driver";
	private static String query;
	
	private static String rdcaddress = "127.0.0.1";
	private static int rdcPort = 4323;
	private static int srvPort = 4322;
	
	
	
	static double minlat;
	static double maxlat ;
	static double minlon ;
	static double maxlon ;
	static String minDate;
	static String maxDate;
	static int topk;
	
	public static void main(String[] args) {
		
		topk = 3;
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
			socket.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}	
	

	//Run query and return resulted lines in a string list
	private static ArrayList<String> runQuery(String query){
		ArrayList<String> poi = new ArrayList<String>();
		
		try {
			Class.forName(dbClass);
				Connection con = DriverManager.getConnection(dbURL);
				Statement stm = con.createStatement();
				ResultSet rs = stm.executeQuery(query);
			while (rs.next()) {
				poi.add(rs.getString(3));
			}
			con.close();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		
		return poi;
	}
	//Map function to find 
	private static Map<String, Long> map(ArrayList<String> poi, int k){
		
		Map<String, Long> pois = poi.parallelStream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
		
		return pois.entrySet()
				.stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(k)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));
	}
	
	
	
	
	static class Client extends Thread{
		ObjectOutputStream out;
		ObjectInputStream in ;
		ObjectOutputStream rout;
		ObjectInputStream rin ;
		Socket client;
		Socket reducer;
		
		public Client(Socket connection){     
			this.client = connection;
			
			
			try {
	
				reducer = new Socket(InetAddress.getByName(rdcaddress), rdcPort);
				out = new ObjectOutputStream(connection.getOutputStream());
				in = new ObjectInputStream(connection.getInputStream());
				rout = new ObjectOutputStream(reducer.getOutputStream());
				rin = new ObjectInputStream(reducer.getInputStream());		
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		@Override
		public void run() {
			ArrayList<String> order;
			Map<String, Long> mapped = null;
			
			try{
			
				try {
					order = (ArrayList<String>) in.readObject();
					query = "SELECT * FROM checkins WHERE latitude >= "
							+order.get(0)+" AND latitude < "+order.get(1)
							+" AND longitude BETWEEN "+order.get(2)+" AND "+order.get(3)
							+" AND time BETWEEN "+order.get(4)+" AND "+order.get(5);
					mapped = map(runQuery(query), topk);
					//System.out.println(Arrays.toString(mapped.entrySet().toArray()));
				} catch (ClassNotFoundException e) {
					System.out.println("Data Received to Unknown format");
				}
			//Send data to reducer
				
			rout.writeObject(mapped);
			rout.flush();
				
			out.writeObject("Done");
			out.flush();
			
			in.close();
			out.close();
			client.close();
			}catch(IOException e){
				System.out.println("Sam ting wong");
			}
		}
	}
	
}


