import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
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

class Mapper extends Thread{
	private static String dbURL = "jdbc:mysql://83.212.117.76:3306/ds_systems_2016?user=omada66&password=omada66db";
	private static String dbClass = "com.mysql.jdbc.Driver";
	private static String query;  
	  
	private static String rdcaddress = "127.0.0.1";
	private static int rdcPort = 4323;
	private static int topk = 4;
	
	ObjectOutputStream out;
	ObjectInputStream in ;
	ObjectOutputStream rout;
	Socket client;
	Socket reducer;
	

	private static Map<CheckIn, Long> mapped;
	
	public Mapper(Socket connection){     
		this.client = connection;
		try {

			reducer = new Socket(InetAddress.getByName(rdcaddress), rdcPort);
			out = new ObjectOutputStream(connection.getOutputStream());
			in = new ObjectInputStream(connection.getInputStream());
			rout = new ObjectOutputStream(reducer.getOutputStream());
			
		} catch (IOException e){
			e.printStackTrace();
		}
		
	}
	@Override
	public void run() {
		
		ArrayList<String> order;
		
		try{
		
			try {
				//Receive order from Client
				order = (ArrayList<String>) in.readObject();
				mapped = map(runQuery(order), topk);
			} catch (ClassNotFoundException e) {
				System.out.println("Data Received to Unknown format");
			}
			
		//Send data to Reducer		
		rout.writeObject(mapped);
		rout.flush();
		
		//Send message to Client to terminate Mapper thread
		out.writeObject("Done");
		out.flush();
		
		in.close();
		out.close();
		client.close();
		}catch(IOException e){
			System.out.println("IO");
		}
	}
	
	//Run query and return resulted checkins in a Checkins object
	private static CheckIns runQuery(ArrayList<String> order){
		
		//Build query String
		query = "SELECT * FROM checkins WHERE latitude >= "
				+order.get(0)+" AND latitude < "+order.get(1)
				+" AND longitude BETWEEN "+order.get(2)+" AND "+order.get(3)
				+" AND time BETWEEN "+order.get(4)+" AND "+order.get(5);
				//+" AND photos <> 'Not exists'";
		
		CheckIns checkins = new CheckIns();
		
		try {
			Class.forName(dbClass);
				Connection con = DriverManager.getConnection(dbURL);
				Statement stm = con.createStatement();
				ResultSet rs = stm.executeQuery(query);
			while (rs.next()) {
				checkins.addCheckin(rs.getString(3), rs.getString(4), rs.getDouble(7),  rs.getDouble(8), rs.getString(10));
			}
			con.close();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		
		return checkins;
	}
	
	//map function to find the top k best on the list returned by query
	private static Map<CheckIn, Long> map(CheckIns checkins, int k){
		
		
		
		 checkins.getCheckins()
				.parallelStream()
                .sorted(new Comparator<CheckIn>() {

					@Override
					public int compare(CheckIn ch1, CheckIn ch2) {
						// TODO Auto-generated method stub
						return ch1.getPhotoURL().size() - ch2.getPhotoURL().size();
					}
                	
				})
                .limit(k)
                .collect(Collectors.toMap(CheckIn::POI,);
               return null;
	}
	
	
}