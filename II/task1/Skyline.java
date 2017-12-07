package task1;
import java.util.*;

public class Skyline {
    public static ArrayList<Tuple> mergePartitions(ArrayList<ArrayList<Tuple>> partitions){
    	ArrayList<Tuple> result = new ArrayList<Tuple>();
    	for (ArrayList<Tuple> partition : partitions){
    		result = Merge(result, partition);
    	}
    	return result;
    }
    
    public static ArrayList<Tuple> Merge(ArrayList<Tuple> S1, ArrayList<Tuple> S2){
    	S1.addAll(S2);
    	return new ArrayList<Tuple>(nlSkyline(S1)) ;
    }
    

    public static ArrayList<Tuple> dcSkyline(ArrayList<Tuple> inputList, int blockSize){
        ArrayList<ArrayList<Tuple>> partitions = new ArrayList<ArrayList<Tuple>>();
    	
    	if (inputList.size() < blockSize){
        	return nlSkyline(inputList);
        } else {
        	ArrayList<Tuple> sub;
        	int cut = inputList.size()/blockSize + 1;
        	for (int i = 0; i < cut ; i ++){
        		if (i == cut - 1)
        			sub = new ArrayList<Tuple>(inputList.subList(i*blockSize, inputList.size()));
        		else
        			sub = new ArrayList<Tuple>(inputList.subList(i*blockSize, (i+1)*blockSize));
        		partitions.add((sub));
        	}
        }
  
    	return new ArrayList<Tuple>(mergePartitions(partitions));
    }

    public static ArrayList<Tuple> nlSkyline(ArrayList<Tuple> partition_) {
    	ArrayList<Tuple> partition = new ArrayList<Tuple>(partition_); // New object because I will remove some object in list
        ArrayList<Tuple> candidates = new ArrayList<Tuple>();
        ArrayList<Tuple> toRemove = new ArrayList<Tuple>();
        Tuple object ;
        boolean gotoWhile = false;
		candidates.add(partition.remove(0));
		
        while (! partition.isEmpty()){

        	object = partition.remove(0);
        	for(Tuple candidate : candidates){
        		if (candidate.dominates(object)) {
        			gotoWhile = true; // Go to while loop and don't execute any line of code after this for loop 
        			break;
        		} else if (object.isIncomparable(candidate)) {
        			continue;
        		} else { // object dominates candidate
        			toRemove.add(candidate);
        		}
        	}
        	if (! gotoWhile) {
        		candidates.removeAll(toRemove);
        		toRemove.clear();
    			candidates.add(object);
    		}
        	gotoWhile = false;
        }
        return candidates;
    }
    
}

class Tuple {
    private int price;
    private int age;  

    public Tuple(int price, int age){
        this.price = price;
        this.age = age;
    }

    public boolean dominates(Tuple other){
        return this.price <= other.price && this.age <= other.age;
    }

    public boolean isIncomparable(Tuple other){
        return !this.dominates(other) && !other.dominates(this);
    }

    public int getPrice() {
        return price;
    }

    public int getAge() {
        return age;
    }

    
    public String toString(){
        return price + "," + age;
    }

    public boolean equals(Object o) {
        if(o instanceof Tuple) {
            Tuple t = (Tuple)o;
            return this.price == t.price && this.age == t.age;
        } else {
            return false;
        }
    }
}