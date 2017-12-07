package t2;

import java.util.Random;

public class betterFrequencyEstimator {

	private int w;
	private int d;
	private static final double E = 2.71 ;
	private int[][] sketches;
	private int[][] hashes;
	private Random rand;
	private final static long PRIME = 179425859 ;
	private final static int MAX = 99999 ;
	
	public betterFrequencyEstimator(int availableSpace, double pr1, double epsilon, double pr2) {

		this.w = (int) Math.ceil(E/epsilon) ;
		this.d = (int) Math.ceil(Math.log(1/pr1)) ;
	
		this.sketches = new int [d][w];
		rand = new Random(System.currentTimeMillis());
		
		// Idea : (hashes[0] * key + hashes[1] ) % w
		this.hashes = new int [d][2];
		for (int i = 0 ; i < d ; i++){
			hashes[i][0] = (int) ( rand.nextInt() * PRIME / (double) MAX );
			hashes[i][1] = (int) ( rand.nextInt() * PRIME / (double) MAX );
		}
	}
	
	public void addArrival(String ip){
		int key = string2Int(ip);
		int hashval = 0;
		
		for (int i = 0 ; i < this.d ; i++) {
			hashval = (hashes[i][0] * key + hashes[i][1]) % this.w;
			if (hashval < 0) // in Java, modulus can return a negative number 
				hashval += this.w ;
			this.sketches[i][hashval]++ ;
		}
		
	}
	
	public int getFreqEstimation(String ip){
		int key = string2Int(ip);
		int result = Integer.MAX_VALUE;
		int hashval = 0;
		
		for (int i = 0 ; i < this.d; i++){
			hashval = (hashes[i][0] * key + hashes[i][1]) % this.w ;
			if (hashval < 0)
				hashval += this.w ;
			result = min(this.sketches[i][hashval], result) ;
		}
		
		return result;
	}
	
	private int min (int a, int b) { return (a <= b)? a : b ; }
	
	private int string2Int(String s) {
		String[] ipAddressInArray = s.split("\\.");
		int result = 0;
		for (int i = 0; i < ipAddressInArray.length; i++) {
			int power = 3 - i;
			int ip = Integer.parseInt(ipAddressInArray[i]);
			result += ip * Math.pow(256, power);
		}

		return result;
	}
	
	
}
