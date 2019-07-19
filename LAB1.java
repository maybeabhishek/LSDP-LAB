import java.util.*;

public class LAB1 {
	public static void main(String[] args){
//		int a,b;
		Scanner s = new Scanner(System.in);
//		System.out.println("Enter num: ");
//		a = s.nextInt();
//		int num[] = new int[50];
//		for(int i = 0; i<a; i++){
//			System.out.println("Enter number "+(i+1)+": ");
//			num[i] = s.nextInt();
//		}
//		System.out.println("Enter number to search: ");
//		b = s.nextInt();
//		int flag=0;
//		for(int i = 0; i<a; i++){
//			if(b==num[i]){
//				flag=1;
//				System.out.println("Found "+b);
//				break;
//			}
//			else
//				flag=0; 
//		}
//		if(flag==0)
//			System.out.println(b+" not found");
//		
		String str,copy;
//		System.out.println("Enter string: ");
//		str = s.next();
//		copy = "";
//		int n = str.length();
//		for(int i=n-1;i>0;i--){
//			copy += str.charAt(i);
//		}
//		System.out.println(copy);
//		
//		
		System.out.println("Enter string: ");
		str = s.nextLine();
		System.out.println("Enter substring to find: ");
		copy = s.nextLine();
		System.out.println("Enter replacing string: ");
		String rep = s.nextLine();
		int count =0;
		for(int i=0; i<str.length(); i++){
			int start = i;
			for(int j =0; j<copy.length(); j++){
				if(copy.charAt(j)!=str.charAt(i)){
					break;
				}
				else{
					i+=1;
					count +=1;
				}
			}
			if(count == copy.length() && count!=0){
				System.out.println("Substring Found from "+start + " to "+ i);
				// Insert replace function
			}
			count = 0;
		}
		
		//Sort a string

		s.close();
	}
}
