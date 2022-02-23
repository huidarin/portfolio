package stubs;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * On input, the reducer receives a word as the key and a set
 * of locations in the form "play name@line number" for the values. 
 * The reducer builds a readable string in the valueList variable that
 * contains an index of all the locations of the word. 
 */
public class IndexReducer extends Reducer<Text, Text, Text, Text> {
	private static final String DELIMITER = ",";
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	
    StringBuilder valueList = new StringBuilder();
    boolean firstValue = true;
    
    for (Text value : values) {
    	if (!firstValue) {
    		valueList.append(DELIMITER);
    	} else {
    		firstValue = false;
    	}
    	valueList.append(value.toString());
    }
    context.write(key, new Text(valueList.toString()));
  }	
}