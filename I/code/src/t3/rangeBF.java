package t3;

import java.util.BitSet;
import java.util.Random;

public class rangeBF {
	
	private BloomFilter mBloomFilter[];

	// m = ceil((n * log(p)) / log(1.0 / (pow(2.0, log(2.0)))));
    // k = ceil(log(2.0) * m / n);
	
	public rangeBF(double pr){
		
		this.mBloomFilter = new BloomFilter[33];
		long nbElems, nbBits;
		int nbHashes;
		
		for (int i = 0 ; i < 33 ; i++) {
			// The first filter[0] is from the root : need 1 hash function
			nbElems = Math.min(400000, (long) Math.pow(2, i));
			nbBits = (long) Math.ceil( (nbElems * Math.log(pr))/ (Math.log( 1 / (Math.pow(2.0, Math.log(2.0))))));
			nbHashes = (int) Math.ceil(Math.log(2.0) * nbBits / nbElems);
			this.mBloomFilter[i] = new BloomFilter(nbHashes, (int)nbBits);			
		}	
	}
	
	public void insertValue(String ip){
		long key = (long)string2Int(ip) - Integer.MIN_VALUE;
		long right = (long) Math.pow(2, 32) - 1 ;
		heapInsert(1, 0, right, key, 0);
	}
	public boolean existsInRange(String ipLeft, String ipRight){
		
		long rangeL = (long)string2Int(ipLeft) - Integer.MIN_VALUE;
		long rangeR = (long)string2Int(ipRight) - Integer.MIN_VALUE;
		long right = (long) Math.pow(2, 32) - 1 ; 
		return heapSearch(1, 0, right, rangeL, rangeR, 0) ;
	}
	
	private void heapInsert(long node, long l, long r, long key, int level){
		
		this.mBloomFilter[level].addElem(node);
		
		if (l == r)
			return ;
		
		long  mid = (l + r)/2 ;
		if (key <= mid)
			heapInsert(2 * node, l, mid, key, level +1 );
		else
			heapInsert(2 * node + 1, mid + 1, r, key, level +1 );
	}
	
	private boolean heapSearch(long node, long l, long r, long rangeL, long rangeR, int level){
		long mid;
		if (r < rangeL || l > rangeR) { // Not overlap 
			return false ;
		} else if (l >= rangeL && r <= rangeR) { // (l, r) is in (rangeL, rangeR) 
			return this.mBloomFilter[level].contains(node);
		} else { // partial overlap
			mid = (l + r) / 2 ; 
			return heapSearch(2 * node, l, mid, rangeL, rangeR, level + 1) || heapSearch(2 * node + 1, mid + 1, r, rangeL, rangeR, level + 1);
		}	
	}

	
	public static int string2Int(String s) {
		String[] ipAddressInArray = s.split("\\.");
		long result = 0;
		for (int i = 0; i < ipAddressInArray.length; i++) {
			int power = 3 - i;
			int ip = Integer.parseInt(ipAddressInArray[i]);
			result += ip * Math.pow(256, power);
		}
		
		return (int)(result + Integer.MIN_VALUE);
	}

}

class BloomFilter {
	
	private int nbHashFunction;
	private BitSet mBitSet;
	private long maxElems;
	private int hashes[][];
	private Random rand;
	private final static long PRIME = 179425859 ;
	private final static int MAX = 99999 ;
	
	public BloomFilter(int nbHash, int nbBits) {
		this.nbHashFunction = nbHash;
		this.maxElems = nbBits;
		this.mBitSet = new BitSet((int)nbBits);
		this.hashes = new int[nbHashFunction][2];
		rand = new Random(System.currentTimeMillis());
		for (int i = 0 ; i < nbHashFunction ; i++) {
			hashes[i][0] = (int) ( rand.nextInt() * PRIME / (double) MAX );
			hashes[i][1] = (int) ( rand.nextInt() * PRIME / (double) MAX );
		}
	}
	
	public void addElem(long key) {
		long hashVal;
		for (int i = 0 ; i < nbHashFunction ; i++){
			hashVal = (hashes[i][0] * key + hashes[i][1]) % this.maxElems;
			if (hashVal < 0) hashVal += this.maxElems;
			this.mBitSet.set((int)hashVal);
		}
	}
	
	public boolean contains(long key) {
		long hashVal;
		for (int i = 0 ; i < nbHashFunction ; i++){
			hashVal = (hashes[i][0] * key + hashes[i][1]) % this.maxElems;
			if (hashVal < 0) hashVal += this.maxElems;
			
			if (! this.mBitSet.get((int)hashVal))
				return false;
		}
		return true;
	}	
}
