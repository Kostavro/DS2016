import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class MapWorker {

	private static int srvPort = 4322;

	
	public static void main(String[] args) {
		startServer();
	}
	
	public static void startServer() {
		ServerSocket socket = null;
		Socket connection = null;
	
		try {
			socket = new ServerSocket(srvPort);
	
			while (true) {
				
				connection = socket.accept();
				Thread t = new Mapper(connection);
				t.start();	
			}
		} catch (IOException e){
			e.printStackTrace();
		}
		finally {
			try {
				socket.close();
			}
			catch (IOException e){
				e.printStackTrace();
			}
		}
	}	
		
}

  