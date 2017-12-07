import java.io.*;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class BloomJoin {
    public static void main(String[] args) {
        ArrayList<R> r = new ArrayList<>();
        ArrayList<S> s = new ArrayList<>();
        for(int i = 0; i<100; i++) {
            r.add(new R(i * 2, i * 3));
            s.add(new S(i * 3, i * 4));
        }
        NodeA n1 = new NodeA(r);
        Node n2 = new NodeB(s);
        n1.run(n2);
        for(RS rs: n1.getResult()) {
            System.out.println(rs);
        }
        System.out.println(n1.getTotalMessageSize());
        System.out.println(n2.getTotalMessageSize());
    }

}

abstract class Node {
    public static final int BUCKETS = 10000;
    private long totalMessageSize;
    private Message inbox;

    /**
     * @param receiver the receiver node.
     * @param msg the message to be sent to the receiver node.
     */
    protected void send(Node receiver, Object msg) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(msg);
            oos.close();
            Message message = new Message(this, receiver, baos.toByteArray());
            totalMessageSize += baos.size();
            receiver.setInbox(message);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void setInbox(Message msg) {
        this.inbox = msg;
    }

    /**
     * @return the last message that delivered to the current node.
     */
    protected Message getInbox() {
        return inbox;
    }

    /**
     * @return the total number of bytes that is sent from the current node.
     */
    public long getTotalMessageSize() {
        return totalMessageSize;
    }
    /**
     * The hash function that should be used by BloomJoin
     */
    public int hashFunction(int i) {
        return i % BUCKETS;
    }
    public abstract void run(Node other);
}

class NodeA extends Node {
    private List<R> data;
    private List<RS> result;
    boolean received;

    public NodeA(List<R> data) {
        this.data = data;
        this.result = new ArrayList<RS>();
        this.received = false;
    }

    public void run(Node other) {
    	BitSet bf = createBF();
    	send(other, bf);
    	// Trigger nodeB run
    	other.run(this);
    	// and waiting for the result
    	Message msg;
    	while( (msg = this.getInbox())!=null && !received){
    		received = true;
    		ArrayList<S> toJoin = (ArrayList<S>) msg.getContent();
    		
    		if (toJoin == null)
    			return;
    		
    		// Join, 
    		for (R r : data)
    			for (S s : toJoin)
    				if (r.getB() == s.getB())
    					result.add(new RS(r.getA(), r.getB(), s.getC()));		
    	}
    }
    
    public BitSet createBF(){
        BitSet bf = new BitSet(BUCKETS);
    	for (R r : data)
    		bf.set(hashFunction(r.getB()));
    	return bf;
    }

    /**
     * @return the result of BloomJoin.
     */
    public List<RS> getResult() {
        return result;
    }
}
class NodeB extends Node {
    private List<S> data;
    boolean received ;
    
    public NodeB(List<S> data) {
        this.data = data;
        this.received = false;
    }
    public void run(Node other) {
    	Message msg;
    	while ((msg = this.getInbox()) !=null && !received){
    		received = true;
    		BitSet bf = (BitSet)msg.getContent();
    		
    		if (bf == null)
    			return;
    		
    		ArrayList<S> dataToJoin = filter(bf);
    		send(other, dataToJoin);
    	}
    }
    
    public ArrayList<S> filter(BitSet bf){
        ArrayList<S> d = new ArrayList<S>();
    	for (S s : data)
    		if (bf.get(hashFunction(s.getB())))
    			d.add(s);
    	return d;
    }

}

class R {
    private int a, b;
    public R(int a, int b) {
        this.a = a;
        this.b = b;
    }

    public int getA() {
        return a;
    }

    public int getB() {
        return b;
    }
}

class S implements Serializable {
    private int b, c;
    public S(int b, int c) {
        this.b = b;
        this.c = c;
    }

    public int getB() {
        return b;
    }

    public int getC() {
        return c;
    }
}

class RS {
    private int a, b, c;
    public RS(int a, int b, int c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    public int getA() {
        return a;
    }

    public int getB() {
        return b;
    }

    public int getC() {
        return c;
    }

    @Override
    public String toString() {
        return "<" + a + ", " + b + ", " + c + ">";
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof RS) {
            RS rs = (RS) obj;
            return a == rs.a && b == rs.b && c == rs.c;
        } else {
            return false;
        }
    }
}


class Message {
    private Node sender;
    private Node receiver;
    private byte[] msg;
    public Message(Node sender, Node receiver, byte[] msg) {
        this.sender = sender;
        this.receiver = receiver;
        this.msg = msg;
    }

    /**
     * @return the object that the message contains.
     */
    public Object getContent() {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(msg);
            ObjectInputStream ois = new ObjectInputStream(bis);
            return ois.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}