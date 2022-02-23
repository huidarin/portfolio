package stubs;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
		      throws IOException, InterruptedException {
		try {
			if (key.get() == 0 && value.toString().contains("customer_id") && value.toString().contains("star_rating") && value.toString().contains("product_id")) {
				return;
			}
			else {
				String line = value.toString();
				String values[] = line.split("\t");
				String customer_id = values[1];
				String product_id = values[3];
				int rating = Integer.parseInt(values[7]);
				if (rating >= 4) {
					context.write(new Text(product_id), new Text(customer_id));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
