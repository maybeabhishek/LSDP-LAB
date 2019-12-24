public class hello{


    
    public static void main(String[] args){
        Student Abhishek = new Student();
        Student Rutuja = new Student();
        Abhishek.regNo = "acjo";
        Rutuja.regNo = "sdsd";
        Abhishek.name ="bewakoof";
        Rutuja.name = "ad";
        // Abhishek.attendance=34;
        // Rutuja.attendance=23;
        Abhishek.setAttendance(34);
        Rutuja.setAttendance(23);
        System.out.print(args[0]);

    }


}

class Student{
    String regNo;
    String name;
    private int attendance;

    void setAttendance(int a){
        attendance = a;
        this.getDetails();
    }
    void getDetails(){
        System.out.println("The name is "+name+" and reg no "+ regNo+" whose attendance is "+attendance+"\n");
    }
}