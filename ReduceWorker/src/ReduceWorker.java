import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

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

