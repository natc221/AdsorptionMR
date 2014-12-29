package friendRecMR;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriendRecInitMapper extends 
Mapper<LongWritable, Text, Text, NodeLabelWritable> {
	
	public FriendRecInitMapper() {
		super();
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
	throws IOException, InterruptedException {
		String line = value.toString();
		String[] split = line.split("\\s+");
		
		Text fromKey = new Text(split[0]);
		Text toKey = new Text(split[1]);
		double edgeWeight = Double.parseDouble(split[2]);
		
		//write A -> B
		NodeLabelWritable fromNode = new NodeLabelWritable();
		fromNode.addAdjacent(toKey, edgeWeight);
		//label for this node is itself
		fromNode.addLabel(fromKey, 1.0);
		context.write(fromKey, fromNode);
		
		//write B, to account for nodes with no outgoing links
		NodeLabelWritable toNode = new NodeLabelWritable();
//		//label for this node is itself
		toNode.addLabel(toKey, 1.0);
		context.write(toKey, toNode);
	}
	
}
