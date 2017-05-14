import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Simple class to hold start, stop values for a location in a sequence
 * @author Michael Hamilton
 *  *
 */
public class SubstringLocation implements Writable{
private int start;
private int stop;

/**
 * Set start, stop position
 * @param start start position of substring
 * @param stop stop position of substring
 */
public SubstringLocation(int start, int stop){
	this.start = start;
	this.stop = stop;
}

/**
 * Default constructor, blah.
 */
public SubstringLocation(){ 
	this.start = 0;
	this.stop = 0;	
}

/**
 * Access start position
 * @return start position
 */
public int getStart() { return start; }

/**
 * Access stop position
 * @return stop position
 */
public int getStop() { return stop; }

/**
 * Read values from stream
 */
public void readFields( DataInput in ) throws IOException{
	this.start = in.readInt();
	this.stop = in.readInt();	
}

/**
 * @param in Input stream stuff
 */
public static SubstringLocation read(DataInput in) throws IOException {
	SubstringLocation sl = new SubstringLocation();
	sl.readFields(in);
	return sl;
}

/**
 * Writes values to stream
 */
public void write(DataOutput out) throws IOException {
	out.writeInt(this.start);
	out.writeInt(this.stop);	
}

}
