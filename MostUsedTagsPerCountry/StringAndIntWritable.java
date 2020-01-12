// Mastère Big Data 2019/2020 - TP Hadoop
//
// Benjamin Thery - benjamin.thery@grenoble-inp.org

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

class StringAndIntWritable implements Comparable<StringAndIntWritable>, Writable {

	public String  strValue;
	public Integer intValue;

	StringAndIntWritable()
	{
		this.strValue = "";
		this.intValue = 0;
	}

	StringAndIntWritable(String strValue, Integer intValue)
	{
		this.strValue = strValue;
		this.intValue = intValue;
	}

	public int compareTo(StringAndIntWritable si)
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

	public void write(DataOutput out) throws IOException
	{
		out.writeUTF(this.strValue);
		out.writeInt(this.intValue);
	}

	public void readFields(DataInput in) throws IOException
	{
		this.strValue = in.readUTF();
		this.intValue = in.readInt();
	}
}
