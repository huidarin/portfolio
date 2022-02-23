package stubs;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class RatingMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text product_id = new Text();
	
	public void map(LongWritable key, Text value, Context context)
		      throws IOException, InterruptedException {
		try {
            if (key.get() == 0 && value.toString().contains("product_id") && value.toString().contains("star_rating"))
                return;
            else {
            	String line = value.toString();
        		String values[] = line.split("\t");
        		product_id.set(values[3]);
        		int rating = Integer.parseInt(values[7]);
        		context.write(new Text(product_id), new IntWritable(rating));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}	
}
	