package t3;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;


public class task3 {
	

	public static void main(String[] args) throws IOException {
		final String path = "....Database Systems/Project/I/code/src/t3/";
		final double PROB = 0.1;
		
		rangeBF bf = new rangeBF(PROB);
		Scanner scanStream = new Scanner(new File(path + "stream.tsv")) ;
		Scanner scanQuery = new Scanner(new File(path + "task3_queries.txt")) ;
		String ipSrc;

		List<Boolean> results = new ArrayList<Boolean>();
		
		while (scanStream.hasNextLine()) {
			ipSrc = scanStream.nextLine().split("\t")[0];
			bf.insertValue(ipSrc);	
		}
		
		String range[];
		
		while (scanQuery.hasNextLine()) {
			range = scanQuery.nextLine().split("\t");
			results.add(bf.existsInRange(range[0], range[1]));
		}
		
		scanQuery.close();
		scanStream.close();

		write(results, path + "out3.txt");
	}
	
	public static void write(List<Boolean> array, String path){
		try {
		    PrintWriter pr = new PrintWriter(path);    
		    pr.print("(");
		    int i;
		    for (i=0; i<array.size() ; i++)
		        pr.print(array.get(i) + ((i == array.size()-1) ? ")" : ","));

		    pr.close();
		} catch (Exception e) {
		    e.printStackTrace();
		    System.out.println("No such file exists.");
		}
	}

}
