package stubs;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<LongWritable, Text, UserPairWritable, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
		      throws IOException, InterruptedException {
		String line = value.toString();
		String[] values = line.split("\t");
		if (values.length == 3) {
			String product = values[0];
			String user1 = values[1];
			String user2 = values[2];
			context.write(new UserPairWritable(user1, user2), new Text(product));
		}
	}
}