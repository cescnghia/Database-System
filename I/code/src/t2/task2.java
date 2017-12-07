package t2;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class task2 {

	public static void main(String[] args) throws IOException {
		final String path = "/Database Systems/Project/I/code/src/t2/";
		final double epsilon = 0.01;
		final double pr1 = 0.1;
		final double pr2 = 0.9;
		final int availableSpace = 10000000;
		
		
		betterFrequencyEstimator sketche = new betterFrequencyEstimator(availableSpace, pr1, epsilon, pr2);
		Scanner scanStream = new Scanner(new File(path + "stream.tsv")) ;
		Scanner scanQuery = new Scanner(new File(path + "task2_queries.txt")) ;
		
		String ipSrc; 
		long time = 0;
		List<Integer> results = new ArrayList<Integer>();
		
		String[] query = scanQuery.nextLine().split("\t");
		while (scanStream.hasNextLine()) {
			time ++;
			ipSrc = scanStream.nextLine().split("\t")[0];
			sketche.addArrival(ipSrc);
			
			if (Integer.parseInt(query[0]) == time) {

				results.add(sketche.getFreqEstimation(query[1]));
	
				if (scanQuery.hasNextLine())
					query = scanQuery.nextLine().split("\t");
				else // exit the program because we don't have query anymore
					break;
			}		
		}
		scanQuery.close();
		scanStream.close();

		write(results, path + "out2.txt");
	}
	
	public static void write(List<Integer> array, String path){
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
