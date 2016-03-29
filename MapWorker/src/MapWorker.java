import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MapWorker {
	private static String dbURL = "jdbc:mysql://83.212.117.76:3306/ds_systems_2016?user=omada66&password=omada66db";
	private static String dbClass = "com.mysql.jdbc.Driver";
	private static String query;
	
	private static int port = 1821;
	private static String address = "localhost";
	
	
	static double minlat ;
	static double maxlat ;
	static double minlon ;
	static double maxlon ;
	static String minDate;
	static String maxDate;
	static int topk;
	
	public static void main(String[] args) {
		
		topk = 4;
		startServer();

	
	}
	
	public static void startServer() {
	ServerSocket socket = null;
	Socket connection = null;
	String message = null;

	try {
		socket = new ServerSocket(4321);

		while (true) {
			// Auto tha mpei se run function kai tha ulopoihthei mia private class client pou tha tin diathetei gia na uparxei polinimatismos
			connection = socket.accept();

			ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
			ObjectInputStream in = new ObjectInputStream(connection.getInputStream());
			out.writeObject("Connection Successful");
			out.flush();

			
				try {
					ArrayList<String> order = (ArrayList<String>) in.readObject();
					query = "SELECT * FROM checkins WHERE latitude BETWEEN "
							+order.get(0)+" AND "+order.get(1)
							+" AND longitude BETWEEN "+order.get(2)+" AND "+order.get(3)
							+" AND time BETWEEN "+order.get(4)+" AND "+order.get(5);
					Map<String, Long> mapped = map(runQuery(query), topk);
					System.out.println(Arrays.toString(mapped.entrySet().toArray()));
				} catch (ClassNotFoundException e) {
					System.out.println("Data Received to Unknown format");
				}
				
				
			in.close();
			out.close();
			connection.close();
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
	
}
