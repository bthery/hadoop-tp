// Mastère Big Data 2019/2020 - TP Hadoop
//
// Benjamin Thery - benjamin.thery@grenoble-inp.org

class StringAndInt implements Comparable<StringAndInt>{
	String  strValue;
	Integer intValue;

	StringAndInt(String strValue, Integer intValue)
	{
		this.strValue = strValue;
		this.intValue = intValue;
	}
	
	public int compareTo(StringAndInt si)
	{
		// MinMaxPriorityQueue automatically evicts greatest element
		// Here we want to _keep_ the greatest element instead, so we revert 
		// the comparison routine.
		if(this.intValue == si.intValue)  
			return 0;  
		else if(this.intValue > si.intValue)  
			return -1;  // See comment above
		else  
			return 1;
	}  
}
