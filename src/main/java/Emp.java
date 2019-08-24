public class Emp {
    public String no;
    public String name;
    public String job;
    public String hireDate;
    public String salary;
    public String comm;
    public String deptNo;
    public String manager;

    public String getNo() {
        return no;
    }

    public void setNo(String no) {
        this.no = no;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public String getHireDate() {
        return hireDate;
    }

    public void setHireDate(String hireDate) {
        this.hireDate = hireDate;
    }

    public String getSalary() {
        return salary;
    }

    public void setSalary(String salary) {
        this.salary = salary;
    }

    public String getComm() {
        return comm;
    }

    public void setComm(String comm) {
        this.comm = comm;
    }

    public String getDeptNo() {
        return deptNo;
    }

    public void setDeptNo(String deptNo) {
        this.deptNo = deptNo;
    }

    public String getManager() {
        return manager;
    }

    public void setManager(String manager) {
        this.manager = manager;
    }

    public Emp(String inputString){
        String[] split = inputString.split(",");
        this.no = (split[0].isEmpty() ? "" : split[0]);
        this.name = (split[1].isEmpty() ? "" : split[1]);
        this.job = (split[2].isEmpty() ? "" : split[2]);
        this.hireDate = (split[3].isEmpty() ? "" : split[3]);
        this.salary = (split[4].isEmpty() ? "" : split[4]);
        this.comm = (split[5].isEmpty() ? "" : split[5]);
        this.deptNo = (split[6].isEmpty() ? "" : split[6]);

        try {
            this.manager = (split[7].isEmpty() ? "" : split[7]);
        }catch (IndexOutOfBoundsException e){
            this.manager = "";
        }
    }
}
