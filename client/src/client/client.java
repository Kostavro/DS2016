package client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

public class client {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		new client().startClient();
	}

	public void startClient() {
		Socket socket = null;
		ObjectInputStream in = null;
		ObjectOutputStream out = null;
		String message;

		try {
			socket = new Socket(InetAddress.getByName("127.0.0.1"), 4321);
			out = new ObjectOutputStream(socket.getOutputStream());
			in = new ObjectInputStream(socket.getInputStream());

			try {
				message = (String) in.readObject();
				System.out.println("Server>" + message);
				
				ArrayList<String> order = new ArrayList<String>();
				order.add("40.78");
				order.add("40.88");
				order.add("-74.9");
				order.add("-73.7");
				order.add("'2012-04-30 00:00:00'");
				order.add("'2012-05-06 00:00:00'");
				out.writeObject(order);
				out.flush();
			} catch (ClassNotFoundException e1) {
				System.err.println("Data Received to Unknown Format");
			}
		} catch (UnknownHostException e2) {
			System.err.println("You are trying to connect to an unknown host");

		} catch (IOException e3) {
			// TODO: handle exception
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