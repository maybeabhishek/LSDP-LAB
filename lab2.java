import java.util.Scanner;
import java.util.*;

public class lab2{
    public static void main(String args[]) {
        Scanner s = new Scanner(System.in);
        System.out.println("Enter String with Whitespace");
        String st = s.nextLine();
        st = removeWhiteSpace(st);
        System.out.println(st);
        detectDuplicate(st);

        
        int n;
        System.out.println("Enter number of strings");
        n = s.nextInt();
        String str[] = new String[n];
        System.out.println("Enter Strings\n");
        for(int i =0; i<n;i++){
            str[i] = s.next();
        }
        detectDuplicateArray(str);
    }

    static String removeWhiteSpace(String st){
        st = st.replaceAll("\\s+", "");
        return st;

    }

    static void detectDuplicate(String st){
        Map<Character, Integer> countMap = new HashMap<Character, Integer>();
        char charArray[] = st.toCharArray();
        for(Character ch: charArray){
            if(countMap.containsKey(ch)){
                countMap.put(ch, countMap.get(ch)+1);
            }
            else
                countMap.put(ch, 1);
        }
        Set<Character> keys = countMap.keySet();  
        for (Character ch : keys) {  
            if (countMap.get(ch) > 1) {  
                System.out.println(ch + "  : " + countMap.get(ch));  
            }  
        }
        System.out.print(countMap);  
    }

    static void detectDuplicateArray(String st[]){
        Map<String, Integer> countMap = new HashMap<String, Integer>();
        for(String s: st){
            if(countMap.containsKey(s)){
                countMap.put(s, countMap.get(s)+1);
            }
            else
                countMap.put(s, 1);
        }
        Set<String> keys = countMap.keySet();  
        for (String ch : keys) {  
            if (countMap.get(ch) > 1) {  
                System.out.println(ch + "  : " + countMap.get(ch));  
            }  
        }

    }
}