package stubs;
import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.FileSplit; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexMapper extends Mapper<Text, Text, Text, Text> {

  @Override
  public void map(Text key, Text value, Context context) throws IOException,
      InterruptedException {
	  
	  FileSplit fileSplit = (FileSplit) context.getInputSplit(); 
	  Path path = fileSplit.getPath(); 	  
	  String line = value.toString();
	  
	  String str = path.getName() + "@" + key.toString();
	  Text location = new Text(str);
	  
	  String lc = line.toLowerCase();
	 
	  for (String word : lc.split("\\W+")) {
		  if (word.length() > 0) {
			  context.write(new Text(word), location);
		  }
	  }
	  
  }
}