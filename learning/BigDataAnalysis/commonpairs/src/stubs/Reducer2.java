package stubs;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<UserPairWritable, Text, UserPairWritable, Text> {
	@Override
	public void reduce(UserPairWritable key, Iterable<Text> values, Context context)
		      throws IOException, InterruptedException {
		ArrayList<String> productsUnique = new ArrayList<String>();
		StringBuilder builder = new StringBuilder();
		int size = 0;
		String delim = "";
		for (Text value : values) {
			String product = value.toString();
			if (!productsUnique.contains(product)) {
				productsUnique.add(product);
				builder.append(delim).append(value);
				delim = ",";
				size++;
			}
		}
		String products = builder.toString();
		if (size >= 3) {
			context.write(key, new Text(products));
		}
	}
}