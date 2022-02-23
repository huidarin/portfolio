package stubs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Hw2Driver {
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.print("Usage: Chain HW 2 Jobs <input dir> <temp output dir> <final output dir>\n");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		
		Job job1 = Job.getInstance(conf, "job1");
		job1.setJarByClass(Hw2Driver.class);
		
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		
		job1.setMapperClass(Mapper1.class);
	    job1.setReducerClass(Reducer1.class);
	    
	    job1.setMapOutputKeyClass(Text.class);
	    job1.setMapOutputValueClass(Text.class);
	    
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(UserPairWritable.class);
	    
	    int flag = job1.waitForCompletion(true) ? 0 : 1;
	    if (flag != 0) {
	    	System.out.print("Job1 failed, exiting");
	    	System.exit(flag);
	    }
	    
	    Job job2 = Job.getInstance(conf, "job2");
	    job2.setJarByClass(Hw2Driver.class);
	    
	    FileInputFormat.setInputPaths(job2, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
	    
	    job2.setMapperClass(Mapper2.class);
	    job2.setReducerClass(Reducer2.class);
	    
	    job2.setMapOutputKeyClass(UserPairWritable.class);
	    job2.setMapOutputValueClass(Text.class);
	    
	    job2.setOutputKeyClass(UserPairWritable.class);
	    job2.setOutputValueClass(Text.class);
	    
	    boolean success = job2.waitForCompletion(true);
	    System.exit(success ? 0 : 1);
	}
}