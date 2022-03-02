package stubs;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, Text, Text, UserPairWritable> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
		      throws IOException, InterruptedException {
		ArrayList<String> users = new ArrayList<String>();
		for (Text value: values) {
			users.add(value.toString());
		}
		for (int i = 0; i < users.size(); i++) {
			for (int y = i + 1; y < users.size(); y++) {
				String user1 = users.get(i);
				String user2 = users.get(y);
				String product = key.toString();
				if (!user1.equals(user2)) {
					if (user1.compareToIgnoreCase(user2) > 0) {
						context.write(new Text(product), new UserPairWritable(user2, user1));
					} else if (user1.compareToIgnoreCase(user2) < 0) {
						context.write(new Text(product), new UserPairWritable(user1, user2));
					}
				}
			}
		}
	}
}