import java.util.*;

/**
 *  implement a (main-memory) data store with MVTO.
 *  objects are <int, int> key-value pairs.
 *  if an operation is to be refused by the MVTO protocol,
 *  undo its xact (what work does this take?) and throw an exception.
 *  garbage collection of versions is not required.
 *  Throw exceptions when necessary, such as when we try to execute an operation in
 *  a transaction that is not running; when we insert an object with an existing
 *  key; when we try to read or write a nonexisting key, etc.
 *  Keep the interface, we want to test automatically!
 *
 **/

/**
 * Explanation for fields in this class
 * 
 * === mData 				 : Map (key, value) with key = object(to store/write) 
 * 							   		maps with a list of its versions
 * === mTransactions 		 : List of all transactions
 * === mLiveList 			 : List of transactions that lives/ not commit yet
 * === mWaitForAnotherCommit : Map (key, value) with key = transaction that can not commit 
 * 									and depending on some commits (its value)
 * === mCommitFail 			 : List of transaction that fail/abort
 *
 */

public class MVTO {
	private static Map<Integer, LinkedList<Version>> mData = new HashMap<Integer, LinkedList<Version>>();
	private static int max_xact = 0;
	private static ArrayList<Transaction> mTransactions = new ArrayList<Transaction>();
	private static ArrayList<Transaction> mLiveList = new ArrayList<Transaction>();
	private static ArrayList<Transaction> mCommitted = new ArrayList<Transaction>();
	private static HashMap<Transaction, ArrayList<Transaction>> mWaitForAnotherCommit = new HashMap<Transaction, ArrayList<Transaction>>();
	private static ArrayList<Transaction> mCommitFail = new ArrayList<Transaction>();


  public static int begin_transaction() { 
	  int xact = max_xact;
	  Transaction tran = new Transaction(xact);
	  mTransactions.add(tran);
	  mLiveList.add(tran);
	  max_xact++;
	  return xact;
  }

  // create and initialize new object in transaction xact
  public static void insert(int xact, int key, int value) throws Exception {
	  System.out.println("T("+(xact+1)+"):I("+(key)+","+value+")");
	  if (mData.containsKey(key)) {
		  System.out.println("T("+(xact+1)+"):ROLLBACK");
		  throw new Exception("KEY ALREADY EXISTS IN T("+(xact+1)+"):I("+key+")");
	  }
	  Version v = new Version(xact, xact, value);
	  LinkedList<Version> m = new LinkedList<Version>();
	  m.add(v);
	  mData.put(key, m);

  }
  
  // Find the version with the largest write timestamp less than or equal to ts
  public static int find_right_version(int ts, int key){
	  int index = 0;
	  LinkedList<Version> versions = mData.get(key);

	  Version result = versions.getFirst();
	  for (int i = 1 ; i < versions.size(); i++)
		  if (versions.get(i).getWTS() <= ts && result.getWTS() <= versions.get(i).getWTS())
			  index = i;
	  return index;
  }

  // return value of object key in transaction xact
  // Store this value in transaction's mReadList and version's mReadList
  // It's important for checking rollback and commit
  public static int read(int xact, int key) throws Exception {
	  if (! mData.containsKey(key))
			throw new Exception("Try to read a non-existing key");
		
	    int index = find_right_version(xact, key);
	    Version v = mData.get(key).get(index);
	    
	    // Change RTS if necessary
	    if (xact > v.getRTS())
	    	mData.get(key).get(index).setRTS(xact);
	    
	    Transaction tran = mTransactions.get(xact);
	    tran.addReadList(new Tuple(key, v.getValue()));
	    mData.get(key).get(index).addReadList(tran);
	    System.out.println("T("+(xact+1)+"):R("+key+") => "+v.getValue());
	    
	    return v.getValue();
  }

  // write value of existing object identified by key in transaction xact
  public static void write(int xact, int key, int value) throws Exception {
	  if (! mData.containsKey(key))
		  throw new Exception("Try to read a non-existing key");
	  Transaction tran = mTransactions.get(xact);
	  if (! mLiveList.contains(tran))
		  throw new Exception("T("+(xact+1)+")DOES NOT EXIST");
	  
	  System.out.println("T("+(xact+1)+"):W("+key+","+value+")");
	  
	  // Add this value into WriteList
	  mTransactions.get(xact).addWriteList(new Tuple(key, value));
		
	  int index = find_right_version(xact, key);
	  Version v = mData.get(key).get(index);
	  
	  // There is another transaction T' with TS' bigger than
	  // TS of transaction T read the value that T' must not read
	  if (xact < v.getRTS()) {
		  rollback(xact);
	  } else {
		  if (xact == v.getWTS()) {
			  mData.get(key).get(index).setValue(value);
		  } else if (xact > v.getWTS()) {
			  Version new_version = new Version(xact, xact, value);
			  LinkedList<Version> versions = mData.get(key);
			  versions.add(new_version);
			  mData.put(key, versions);
		  }
	  }
  }
  
  // Check if there is some transaction T' with TS' > TS AND
  // T' has read a value that after wiil be written by T
  public static boolean check_write_versions(int xact, int key){
	  for (Version version : mData.get(key)){
		  for (Transaction tran : version.getReadList()){
			  if (version.getWTS() <= xact && xact <= tran.getXact())
				  return false;
		  }
	  }
	  return true;
  }

  public static void commit(int xact)   throws Exception {
	  Transaction tran = mTransactions.get(xact);
	  
	  // This transaction has already committed
	  if (mCommitted.contains(tran))
		  return;

	  // This transaction waits for another transactions commit => don't need to print
	  if (! mWaitForAnotherCommit.containsKey(tran))
		  System.out.println("T("+(xact+1)+"):COMMIT START");

	  // This transaction does not exist
	  if (! mLiveList.contains(tran))
		  throw new Exception("T("+(xact+1)+")DOES NOT EXIST");
	  
	  
	  LinkedList<Tuple> writes = tran.getWriteList();
	  LinkedList<Tuple> reads = tran.getReadList();
	  
	  // Check if commit depend on another commit ?
	  for (Tuple t : reads){
		  for (Version v : mData.get(t.getObject())){
			  if (v.getReadList().contains(tran))
				  if (v.getWTS() != xact && !mCommitted.contains(mTransactions.get(v.getWTS()))){
					  ArrayList<Transaction> value = mWaitForAnotherCommit.getOrDefault(tran, new ArrayList<Transaction>());
					  value.add(mTransactions.get(v.getWTS()));
					  mWaitForAnotherCommit.put(tran, value);
					  return;
				  } 
		  }		
	  }
	  
	  // Check if write is violated ?
	  for (Tuple t : writes){
		  if (! check_write_versions(xact, t.getObject())){
			  System.out.println("T("+(xact+1)+"):COMMIT UNSUCCESSFUL");
			  rollback(xact);
		  }
	  }
	  
	  mCommitted.add(tran);
	  mLiveList.remove(tran);
	  System.out.println("T("+(xact+1)+"):COMMIT FINISH");
	  
	  // Before finishing, try to commit for some transactions that waits for this transaction
	  for (Transaction x : mWaitForAnotherCommit.keySet()){
		  if (mWaitForAnotherCommit.get(x).contains(tran))
			  commit(x.getXact());
	  }

  }


  public static void rollback(int xact) throws Exception {
	  Transaction tran = mTransactions.get(xact);
	  System.out.println("T("+(xact+1)+"):ROLLBACK");
	  mCommitFail.add(tran);
	  mLiveList.remove(tran);
	  
	  // This transaction is fail => another transactions that wait for this transaction commits will be also fail
	  for (Transaction x : mWaitForAnotherCommit.keySet()){
		  if (mWaitForAnotherCommit.get(x).contains(tran))
			  System.out.println("T("+(x.getXact()+1)+"):COMMIT UNSUCCESSFUL");
	  }
	  
	  // CASCADING ABORT
	  // For all values written by xact, check if there is another transaction has read these values
	  LinkedList<Tuple> writes = tran.getWriteList();
	  Tuple cause = writes.getLast();
	  int key = cause.getObject();
	  for (Version version : mData.get(key)){
		  // Version that this transaction wrote into
		  if (version.getWTS()==xact){
			  // For all transactions that read this version
			  for (Transaction t : version.getReadList()){
				  if (mLiveList.contains(t)){
					  rollback(t.getXact());
					  throw new Exception("ROLLBACK T("+(xact+1)+"):W("+cause.getObject()+","+cause.getValue()+")");
				  }
			  }
		  // Version that there is some transaction T' st TS' > TS of this transaction
	 	  // and T' has read the value so T cannot write
		  } else if (version.getRTS() > xact){
			  throw new Exception("ROLLBACK T("+(xact+1)+"):W("+cause.getObject()+","+cause.getValue()+")");
		  }
	  }
  }
  
}

/**
 * Explanation for fields in this class
 * 
 * ===mWriteList : List of all Tuple that that this transaction wrote
 * ===mReadList  : List of all Tuple that that this transaction read
 * 
 */

class Transaction {
	private LinkedList<Tuple> mWriteList;
	private LinkedList<Tuple> mReadList;
	private int xact;
	
	public Transaction(int xact){
		this.xact = xact;
		this.mWriteList = new LinkedList<Tuple>();
		this.mReadList = new LinkedList<Tuple>();
	}
	public void addWriteList(Tuple t) {this.mWriteList.add(t);}
	public void addReadList(Tuple t) {this.mReadList.add(t);}
	
	public LinkedList<Tuple> getWriteList() {return this.mWriteList;}
	public LinkedList<Tuple> getReadList() {return this.mReadList;}
	public int getXact() {return this.xact;}
}

/**
 * Explanation for fields in this class
 * 
 * ===mObject : Object (to read/write on)
 * ===mValue  : Value of this object
 * 
 */

class Tuple {
	private int mObject;
	private int mValue;
	
	public Tuple(int o, int v){
		this.mObject = o;
		this.mValue = v;
	}
	
	public int getObject() {return this.mObject;}
	public int getValue() {return this.mValue;}
}

/**
 * Explanation for fields in this class
 * 
 * ===mWTS 		: Write timestamp
 * ===mRTS  	: Read timestamp
 * ===mValue	: Value
 * ===mReadList	: List of all transaction that has read this value
 * 
 * 
 */

class Version {
	private int mWTS;
	private int mValue;
	private int mRTS;
	private LinkedList<Transaction> mReadList;
	
	public Version(int wts, int rts, int value){
		this.mWTS = wts;
		this.mValue = value;
		this.mRTS = rts;
		mReadList = new LinkedList<Transaction>();
	}
	
	public int getWTS() {return this.mWTS;}
	public int getRTS() {return this.mRTS;}
	public int getValue() {return this.mValue;}
	
	public LinkedList<Transaction> getReadList() {return this.mReadList;}
	public void addReadList(Transaction transaction) {this.mReadList.add(transaction);}
	
	public void setWTS(int v) {this.mWTS = v;}
	public void setRTS(int v) {this.mRTS = v;}
	public void setValue(int v) {this.mValue = v;}

}
