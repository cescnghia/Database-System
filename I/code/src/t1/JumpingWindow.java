package t1;

public class JumpingWindow {

	private int subWindowSize;
	private SubWindow subWindows[];
	private SubWindow extraSubWindow; // an extra sub-window in case of all sub-windows is full
	private int actuallyWindow; // in which sub-window you are now
	private int nbSubWindow;
	
	public JumpingWindow(int windowSizeW, double epsilon){
		
		this.subWindowSize = (int)Math.ceil(2 * epsilon * windowSizeW);
		this.nbSubWindow = (int) Math.ceil((double)windowSizeW / subWindowSize);
		this.subWindows = new SubWindow[nbSubWindow];
		for(int i = 0 ; i < nbSubWindow ; i++)
			subWindows[i] = new SubWindow(subWindowSize);
		this.extraSubWindow = new SubWindow(subWindowSize);
		this.actuallyWindow = 0;
	}
	
	void insertEvent(String srcIP){
		int prefix = Integer.parseInt(srcIP.split("\\.")[0]);

		if (extraSubWindow.isFull()) { // no more place => jump window
			// copy
			for (int i = 0 ; i< nbSubWindow - 1 ; i++)
				subWindows[i] = subWindows[i+1];
			subWindows[nbSubWindow-1] = extraSubWindow;
			
			// now we can insert new event into extra window
			extraSubWindow = new SubWindow(subWindowSize);
			extraSubWindow.addEvent(prefix);
		} else if (subWindows[nbSubWindow-1].isFull()) { // all sub windows is now full => insert to extra sub window
			extraSubWindow.addEvent(prefix);
		} else { // insert to sub window
			if (subWindows[actuallyWindow].isFull()) //Move to next sub window if necessary
				actuallyWindow ++;
			subWindows[actuallyWindow].addEvent(prefix);
		}
	}

	// get estimation by taking into account the "running sum" 
	// estimation = 1/2 sum of the "running sum" + 1/2 sum of the last one (partially expiring)
	int getFreqEstimation(String srcIP, int queryWindowSizeW1){
		int ip = Integer.parseInt(srcIP.split("\\.")[0]);
		int result = 0;
		
		if (extraSubWindow.isEmpty()) { // No entry in extra sub-window
			result += getEstimationFromSubWindow(ip, queryWindowSizeW1);
		} else {
			queryWindowSizeW1 -= extraSubWindow.getNbEvents();	
			// 1/2 weight of the "running sum"
			result += 0.5 * extraSubWindow.getEstimateWithRange(ip, extraSubWindow.getNbEvents());
			result += getEstimationFromSubWindow(ip, queryWindowSizeW1);	
		}	
		return result;
	}
	
	int getEstimationFromSubWindow(int ip , int queryWindowSizeW1){
		int result = 0;
		int nbSubWindowNeed = (int)Math.floor(queryWindowSizeW1 / (double)subWindowSize);
		int rangeSubWindow = 0; // Estimate for a fraction of sub-window
		
		// 1/2 weight of the last one (partially expiring)
		result += 0.5 * subWindows[nbSubWindow - nbSubWindowNeed - 1].getEstimateWithRange(ip, rangeSubWindow) ;

		for (int i = nbSubWindow - 1 ; i >= nbSubWindow - nbSubWindowNeed ; i --)
			result += subWindows[i].getNbEventsFromIP(ip);
		

		return result;
	}
}

class SubWindow {
	private int[] window;
	private int capacity;
	private int nbEvents;
	private final int IP_RANGE = 256;
	
	public SubWindow(int size) {
		this.window = new int[IP_RANGE];
		this.capacity = size;
		this.nbEvents = 0;
	}
	
	public void addEvent(int ip) {
		window[ip]++ ;
		nbEvents ++ ;
	}
	
	// Try to estimate how many packets come from IP among n packets (n < capacity)
	public int getEstimateWithRange(int ip, int n) {
		return (int)Math.ceil(((n * window[ip])/(double)capacity)) ;
	}
	
	public boolean isEmpty() { return 0 == this.nbEvents; }
	
	public int getNbEvents() { return this.nbEvents; }
	
	public int getNbEventsFromIP(int ip) { return window[ip]; }
	
	public boolean isFull() { return this.nbEvents == this.capacity ; }
}
