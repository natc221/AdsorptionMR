package friendRecMR;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FriendRecDriver extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new FriendRecDriver(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		String command = args[0];
		String input = args[1];
		String output = args[2];
		int numReducers = Integer.parseInt(args[3]);
		if (command.equals("init")) {
			return runInit(input, output, 10);
		} else if (command.equals("iter")){
			return runIter(input, output, numReducers);
		//command, in, out, numReducers, intermediate, numIterations
		} else if (command.equals("iterTimes")) {
			String interPath = args[4];
			int iterations = Integer.parseInt(args[5]);
			boolean swap = false;
			for (int i = 0; i < iterations - 1; i++) {
				if (swap) {
					runIter(interPath, input, numReducers);
					swap = !swap;
				} else {
					runIter(input, interPath, numReducers);
					swap = !swap;
				}
			}
			//final iteration writes to output
			if (swap) {
				return runIter(interPath, output, numReducers);
			} else {
				return runIter(input, output, numReducers);
			}
		} else if (command.equals("finish")) {
			return runFinish(input, output, numReducers);
		//command, input, output, numReducers, inter1, inter2, iterations
		} else if (command.equals("composite")) {
			String interPath = args[4];
			String interPath2 = args[5];
			//INITIALIZE
			runInit(input, interPath, numReducers);
			//ITERATIONS
			int iterations = Integer.parseInt(args[6]);
			boolean swap = false;
			for (int i = 0; i < iterations; i++) {
				if (swap) {
					runIter(interPath2, interPath, numReducers);
					swap = !swap;
				} else {
					runIter(interPath, interPath2, numReducers);
					swap = !swap;
				}
			}
			//FINISH
			if (swap) {
				return runFinish(interPath2, output, numReducers);
			} else {
				return runFinish(interPath, output, numReducers);
			}
		} else return 1;
	}
	
	private Job createRankJob(String input, String output, int numReducers) 
					throws IllegalArgumentException, IOException {
		@SuppressWarnings("deprecation")
		Job rankJob = new Job(super.getConf());
		// Configure the number of reducers
		rankJob.setNumReduceTasks(numReducers);
		rankJob.setJarByClass(FriendRecDriver.class);

		// set the input and output paths
		FileInputFormat.addInputPath(rankJob, new Path(input));
		Path outputPath = new Path(output);
		FileOutputFormat.setOutputPath(rankJob, outputPath);

		FileSystem fs = FileSystem.get(rankJob.getConfiguration());		
		if (fs.exists(outputPath))
			fs.delete(outputPath, true); // delete file, true for recursive
		return rankJob;
	}
	
	private int runInit(String input, String output, int numReducers) 
			throws IllegalArgumentException, IOException, 
			InterruptedException, ClassNotFoundException {
		
		Job job = createRankJob(input, output, numReducers);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NodeLabelWritable.class);
		
		job.setMapperClass(FriendRecInitMapper.class);
		job.setReducerClass(FriendRecInitReducer.class);
		
		if (job.waitForCompletion(true) == false) {
			return 1;
		}
		else {
			return 0;
		}
	}

	private int runIter(String input, String output, int numReducers) 
			throws IllegalArgumentException, IOException, 
			InterruptedException, ClassNotFoundException {
		Job job = createRankJob(input, output, numReducers);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NodeLabelWritable.class);
		
		job.setMapperClass(FriendRecIterMapper.class);
		job.setReducerClass(FriendRecIterReducer.class);
		
		if (job.waitForCompletion(true) == false) {
			return 1;
		}
		else {
			return 0;
		}
	}
	
	private int runFinish(String input, String output, int numReducers) 
			throws IllegalArgumentException, IOException, 
			InterruptedException, ClassNotFoundException {
		Job job = createRankJob(input, output, numReducers);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NodeLabelWritable.class);
		
		job.setMapperClass(FriendRecFinishMapper.class);
		job.setReducerClass(FriendRecFinishReducer.class);
		
		if (job.waitForCompletion(true) == false) {
			return 1;
		}
		else {
			return 0;
		}
	}
	
	
}







