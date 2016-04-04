import java.util.ArrayList;

class CheckIn {
	public String getPOI() {
		return POI;
	}

	public void setPOI(String pOI) {
		POI = pOI;
	}

	public String getPOI_name() {
		return POI_name;
	}

	public void setPOI_name(String pOI_name) {
		POI_name = pOI_name;
	}

	public double getLantitude() {
		return lantitude;
	}

	public void setLantitude(double lantitude) {
		this.lantitude = lantitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public ArrayList<String> getPhotoURL() {
		return photoURL;
	}

	public void setPhotoURL(ArrayList<String> photoURL) {
		this.photoURL = photoURL;
	}

	private String POI;
	private String POI_name;
	private double lantitude;
	private double longitude;
	private ArrayList<String> photoURL;

	public CheckIn(String POI, String POI_name, double lantitude, double longitude,String photoURL) {
		this.POI = POI;
		this.POI_name = POI_name;
		this.lantitude = lantitude;
		this.longitude = longitude;
		this.photoURL = new ArrayList<String>();
		this.photoURL.add(photoURL);
	}


}