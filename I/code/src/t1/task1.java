package t1;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class task1 {

	public static void main(String[] args) throws IOException {
		final String path = "/Database Systems/Project/I/code/src/t1/";
		final double epsilon = 0.01;
		final int windowSize = 10000;
		
		JumpingWindow jumpWindow = new JumpingWindow(windowSize, epsilon);
		Scanner scanStream = new Scanner(new File(path + "stream.tsv")) ;
		Scanner scanQuery = new Scanner(new File(path + "task1_queries.txt")) ;
		String ipSrc; 
		long time = 0;
		int range;
		List<Integer> results = new ArrayList<Integer>();
		
		String[] query = scanQuery.nextLine().split("\t");
		while (scanStream.hasNextLine()) {
			time ++;
			ipSrc = scanStream.nextLine().split("\t")[0];
			jumpWindow.insertEvent(ipSrc);
			
			if (Integer.parseInt(query[0]) == time) {
				range = Integer.parseInt(query[2]);
				if (0 == range)
					range = windowSize;

				results.add(jumpWindow.getFreqEstimation(query[1], range));
	
				if (scanQuery.hasNextLine())
					query = scanQuery.nextLine().split("\t");
				else // exit the program because we don't have query anymore
					break;
			}		
		}
		scanQuery.close();
		scanStream.close();

		write(results, path + "out1.txt");
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
