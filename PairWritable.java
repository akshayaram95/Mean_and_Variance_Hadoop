
import org.apache.hadoop.io.Writable;
import java.io.*;

public class PairWritable implements Writable{
	private long star;
	private long count;
	private long product_sum;

	public PairWritable(){
	}

	public void write(DataOutput out) throws IOException{
		out.writeLong(star);
		out.writeLong(count);
		out.writeLong(product_sum);
	}

	public void readFields(DataInput in) throws IOException{
		star = in.readLong();
		count = in.readLong();
		product_sum=in.readLong();
	}

	public long getStar(){
		return star;
	}

	public long getCount(){
		return count;
	}


	public long getProduct_Sum(){
		return product_sum;
	}

	public void set(long star, long count, long product_sum){
		this.star = star;
		this.count = count;
		this.product_sum=product_sum;
		
	}
}