import java.util.ArrayList;

public class CheckIns{
	
	public ArrayList<CheckIn> getCheckins() {
		return checkins;
	}
	public void setCheckins(ArrayList<CheckIn> checkins) {
		this.checkins = checkins;
	}

	ArrayList<CheckIn> checkins;
	
	public CheckIns(){
		checkins = new ArrayList<CheckIn>();
	}
	public void addCheckin(String POI, String POI_name, double lantitude, double longitude,String photoURL){
		if(exists(POI)){
			checkins.get(indexOf(POI)).getPhotoURL().add(photoURL);
		}
		else{
			checkins.add(new CheckIn(POI, POI_name, lantitude, longitude,photoURL));
		}
	}
	private boolean exists(String POI){
		for (CheckIn checkIn : checkins) {
			if(POI.equals(checkIn.getPOI())){
				return true;
			}
		}
		return false;
	}
	
	private int indexOf(String POI){
		for (int i = 0; i < checkins.size(); i++) {
			if(POI.equals(checkins.get(i).getPOI())){
				return i;
			}
		}
		return -1;
	}
}
