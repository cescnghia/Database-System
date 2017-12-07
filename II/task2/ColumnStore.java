package org.sparkexample;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class ColumnStore {

	public static void main(String[] args) {
		
		String master = "local[4]";

	    /*
	     * Initializes a Spark context.
	     */
	    SparkConf conf = new SparkConf()
	        .setAppName(ColumnStore.class.getName())
	        .setMaster(master);
	    JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		
		String input 			= args[0];
		String output 			= args[1];
		String schema 			= args[2];
		String projectionList 	= args[3];
		String whereList 		= args[4];
		BufferedReader buffer = null;
		String line = "";
		int count = 0;
		String[] attrs = schema.split(",");
		String[] primitif;
		Position position = null;
		
		ArrayList<JavaRDD<String>> stringsRDD = new ArrayList<JavaRDD<String>>();
		ArrayList<JavaRDD<Float>> floatsRDD = new ArrayList<JavaRDD<Float>>();
		ArrayList<JavaRDD<Integer>> integersRDD = new ArrayList<JavaRDD<Integer>>();
		
		
		
		
		// Read input and put into RDD
		try {
            buffer = new BufferedReader(new FileReader(input));
            while ((line = buffer.readLine()) != null) {

                String[] data = line.split(",");
                
                if (count == 0){ // Initialization RDD
                	count ++;
                	for(int i = 0; i < attrs.length; i++){
                		primitif = attrs[i].split(":");
                		if (primitif[1].equals("Int")){
                			integersRDD.add(sc.parallelize(Arrays.asList(Integer.parseInt(data[i]))));
                		} else if (primitif[1].equals("Float")) {
                			floatsRDD.add(sc.parallelize(Arrays.asList(Float.parseFloat(data[i]))));
                		} else {
                			stringsRDD.add(sc.parallelize(Arrays.asList(data[i])));
                		}
                	}
                } else { // Union into existence RDD
                	for(int i = 0; i < attrs.length; i++){
                		primitif = attrs[i].split(":");
                		position = getPositionRDD(primitif[0], schema);
                		if(position.getArray() == 0) 
                			stringsRDD.set(position.getPosition(), 
                					stringsRDD.get(position.getPosition()).union(sc.parallelize(Arrays.asList(data[i]))));
                		else if (position.getArray() == 1) 
                			integersRDD.set(position.getPosition(), 
                					integersRDD.get(position.getPosition()).union(sc.parallelize(Arrays.asList(Integer.parseInt(data[i])))));
                		else
                			floatsRDD.set(position.getPosition(), 
                					floatsRDD.get(position.getPosition()).union(sc.parallelize(Arrays.asList(Float.parseFloat(data[i])))));
                	}
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (buffer != null) {
                try {
                    buffer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
		
		
		
		// Analyse projectionList
		
		ArrayList<Position> attrsNeeded = new ArrayList<Position>();
		for(String attr: projectionList.split(","))
			attrsNeeded.add(getPositionRDD(attr, schema));
		
		
		//Analyse whereList
		ArrayList<ArrayList<Long>> goodIndexes = new ArrayList<ArrayList<Long>>();
		Position pos;
		String params[];

		
		for(String exp : whereList.split(",")){
			params = exp.split("\\|");
			pos = getPositionRDD(params[0], schema);
			goodIndexes.add(getIndexes(pos, params[1], params[2], stringsRDD, integersRDD, floatsRDD));
		}
		
		// Now I have to take the intersection of all goodIndexes
		
		ArrayList<Long> solution = new ArrayList<Long>(goodIndexes.get(0));
		for (int i = 1 ; i <goodIndexes.size(); i++)
			solution = intersection(solution, goodIndexes.get(i));
		
		
		// Write the result
		String COMMA_DELIMITER = ",";
		String NEW_LINE_SEPARATOR = "\n";
		Collections.sort(solution);
		JavaPairRDD<String, Long> stringPair;
		JavaPairRDD<Integer, Long> intPair ;
		JavaPairRDD<Float, Long> floatPair;
		try {
			FileWriter fw = new FileWriter(output);
			for(Long i : solution){
				for (Position p : attrsNeeded){

					if (p.getArray()==0) {
						stringPair =  stringsRDD.get(p.getPosition()).zipWithIndex();
						fw.append(stringPair.filter(x -> x._2==i).collect().get(0)._1()) ;
					} else if (p.getArray()==1){
						intPair =  integersRDD.get(p.getPosition()).zipWithIndex();
						fw.append(String.valueOf(intPair.filter(x -> x._2==i).collect().get(0)._1()));
					} else {
						floatPair =  floatsRDD.get(p.getPosition()).zipWithIndex();
						fw.append(String.valueOf(floatPair.filter(x -> x._2==i).collect().get(0)._1()));		
					}
					fw.append(COMMA_DELIMITER);
				}
				fw.append(NEW_LINE_SEPARATOR);
				
			}
			fw.flush();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	
	
	public static Position getPositionRDD(String attr,String schema){
		int array = 0;
		int position = 0;
		int countS = 0, countI = 0, countF = 0;
		String[] attrs = schema.split(",");
		String[] primitif;
		for(int i = 0 ; i < attrs.length; i++){
			primitif = attrs[i].split(":");
			
			if(primitif[1].equals("String")) {
				countS++;
				array = 0;
				
			} else if (primitif[1].equals("Int")) {
				countI++;
				array = 1;
			
			} else{
				countF++;
				array = 2;
			}
			
			
			if(primitif[0].equals(attr)){
				position = (attr.equals("String")) ? countS : ((attr.equals("Int")) ? countI : countF);
				break;
			}
			
		}
		return new Position(array, position);
	}
	
	/*
	 * For each comparision, I will look for a array of "good/feasible solution" in terms of indexes
	 */
	public static ArrayList<Long> getIndexes(Position pos, String comparision, String compareTo,
			ArrayList<JavaRDD<String>> stringsRDD, ArrayList<JavaRDD<Integer>> integersRDD, ArrayList<JavaRDD<Float>> floatsRDD){
		ArrayList<Long> indexes = new ArrayList<Long>();
		JavaPairRDD<String, Long> stringPair =  stringsRDD.get(pos.getPosition()).zipWithIndex();
		JavaPairRDD<Integer, Long> intPair =  integersRDD.get(pos.getPosition()).zipWithIndex();
		JavaPairRDD<Float, Long> floatPair =  floatsRDD.get(pos.getPosition()).zipWithIndex();

		if (comparision.equals("=")) {
			if (pos.getArray()==0){
				for(Tuple2<String, Long> tuples :stringPair.filter(x -> x._1.compareTo(compareTo) == 0).collect())
					indexes.add(tuples._2);
			} else if (pos.getArray()==1) {
				for(Tuple2<Integer, Long> tuples :intPair.filter(x -> x._1 == Integer.parseInt(compareTo)).collect())
					indexes.add(tuples._2);
			} else {
				for(Tuple2<Float, Long> tuples :floatPair.filter(x -> x._1 == Float.parseFloat(compareTo)).collect())
					indexes.add(tuples._2);
			}
		} else if (comparision.equals("<")) {
			if (pos.getArray()==0){
				for(Tuple2<String, Long> tuples :stringPair.filter(x -> x._1.compareTo(compareTo) < 0).collect())
					indexes.add(tuples._2);
			} else if (pos.getArray()==1) {
				for(Tuple2<Integer, Long> tuples :intPair.filter(x -> x._1 < Integer.parseInt(compareTo)).collect())
					indexes.add(tuples._2);
			} else {
				for(Tuple2<Float, Long> tuples :floatPair.filter(x -> x._1 < Float.parseFloat(compareTo)).collect())
					indexes.add(tuples._2);
			}
		} else if (comparision.equals("<=")) {
			if (pos.getArray()==0){
				for(Tuple2<String, Long> tuples :stringPair.filter(x -> x._1.compareTo(compareTo) == 0 || x._1.compareTo(compareTo) < 0).collect())
					indexes.add(tuples._2);
			} else if (pos.getArray()==1) {
				for(Tuple2<Integer, Long> tuples :intPair.filter(x -> x._1 <= Integer.parseInt(compareTo)).collect())
					indexes.add(tuples._2);
			} else {
				for(Tuple2<Float, Long> tuples :floatPair.filter(x -> x._1 <= Float.parseFloat(compareTo)).collect())
					indexes.add(tuples._2);
			}
		} else if (comparision.equals(">")) {
			if (pos.getArray()==0){
				for(Tuple2<String, Long> tuples :stringPair.filter(x -> x._1.compareTo(compareTo) > 0).collect())
					indexes.add(tuples._2);
			} else if (pos.getArray()==1) {
				for(Tuple2<Integer, Long> tuples :intPair.filter(x -> x._1 > Integer.parseInt(compareTo)).collect())
					indexes.add(tuples._2);
			} else {
				for(Tuple2<Float, Long> tuples :floatPair.filter(x -> x._1 > Float.parseFloat(compareTo)).collect())
					indexes.add(tuples._2);
			}
		} else { //>=
			if (pos.getArray()==0){
				for(Tuple2<String, Long> tuples :stringPair.filter(x -> x._1.compareTo(compareTo) == 0 || x._1.compareTo(compareTo) > 0).collect())
					indexes.add(tuples._2);
			} else if (pos.getArray()==1) {
				for(Tuple2<Integer, Long> tuples :intPair.filter(x -> x._1 >= Integer.parseInt(compareTo)).collect())
					indexes.add(tuples._2);
			} else {
				for(Tuple2<Float, Long> tuples :floatPair.filter(x -> x._1 >= Float.parseFloat(compareTo)).collect())
					indexes.add(tuples._2);
			}
		}
		return indexes;
	}
	
	public static ArrayList<Long> intersection(ArrayList<Long> list1, ArrayList<Long> list2) {
		ArrayList<Long> list = new ArrayList<Long>();

        for (Long t : list1) {
            if(list2.contains(t)) {
                list.add(t);
            }
        }

        return list;
    }

}


// array = 0 : StringsRDD, array = 1 : IntegersRDD, array = 2 : FloatsRDD
// position = position of that rdd in ArrayList
class Position{
	private int array;
	private int position;
	
	public Position(int array, int position){
		this.array=array;
		this.position=position;
	}
	
	public int getArray() {return this.array;}
	public int getPosition(){return this.position;}
}
