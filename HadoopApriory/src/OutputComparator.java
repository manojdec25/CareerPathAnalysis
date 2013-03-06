import java.util.Comparator;

public class OutputComparator implements Comparator<String>{
 
    @Override
    public int compare(String output1, String output2) {
 
        int length1 = output1.length();
        int length2 = output2.length();
 
        if (length1 > length2){
            return -1;
        }else if (length1 < length2){
            return +1;
        }else{
            return 0;
        }
    }
}