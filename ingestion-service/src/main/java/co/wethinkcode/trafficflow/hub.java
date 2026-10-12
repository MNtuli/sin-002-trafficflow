package co.wethinkcode.trafficflow;

public class hub {
    String id;
    String District;
    String signalType;
    boolean activeflag;

    public hub(String id,String district,String signalType,boolean activeflag){
        this.id = id;
        this.District = district;
        this.signalType = signalType;
        this.activeflag = activeflag;
    }

    public String getId() {
        return id;
    }

    public String getDistrict() {
        return District;
    }

    public String getSignalType() {
        return signalType;
    }
    public boolean getActiveflag(){
        return activeflag;
    }
    @Override
    public String toString() {
        return "hub[id=" + id + ", district=" + District +
                ", signalType=" + signalType + ", active=" + activeflag + "]";
    }
}
