package stubs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgCustomerReviews {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
		      System.out.printf("Usage: AvgWordLength <input dir> <output dir>\n");
		      System.exit(-1);
		    }
	
	Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "avgcustomerreview");
    job.setJarByClass(AvgCustomerReviews.class);
    
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(RatingMapper.class);
    job.setReducerClass(AverageReducer.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputKeyClass(Text.class);
    
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
	}
}