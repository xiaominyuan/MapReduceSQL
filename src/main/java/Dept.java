public class Dept {
    public String deptNo;
    public String deptName;
    public String location;

    public String getDeptNo() {
        return deptNo;
    }

    public void setDeptNo(String deptNo) {
        this.deptNo = deptNo;
    }

    public String getDeptName() {
        return deptName;
    }

    public void setDeptName(String deptName) {
        this.deptName = deptName;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public Dept(String inputString){
        String[] split = inputString.split(",");
        this.deptNo = (split[0].isEmpty() ? "" : split[0]);
        this.deptName = (split[1].isEmpty() ? "" : split[1]);
        try {
            this.location = (split[2].isEmpty() ? "" : split[2]);
        }catch (IndexOutOfBoundsException e){
            this.location = "";
        }
    }
}
