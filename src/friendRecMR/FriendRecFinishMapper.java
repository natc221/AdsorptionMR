package friendRecMR;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriendRecFinishMapper 
extends Mapper<LongWritable, Text, Text, NodeLabelWritable> {
	
	public FriendRecFinishMapper() {
		super();
	}
	
	@Override
	public void map(LongWritable key, Text val, Context context) 
			throws IOException, InterruptedException {
		String line = val.toString();
		String[] split = line.split("\\s+");
		
		Text nodeID = new Text(split[0]);
		
		MapWritable adjNodes = new MapWritable();
		MapWritable labels = new MapWritable();
		
		//read adjacent nodes and labels
		boolean readNodes = true;
		for (int i = 1; i < split.length; i++) {
			String s = split[i];
			if (s.equals("adj:")) continue;
			if (s.equals("labels:")) {
				readNodes = false;
				continue;
			}
			s = s.replace("(", "");
			s = s.replace(")", "");
			String[] vals = s.split(",");
			if (readNodes) {
				adjNodes.put(new Text(vals[0]), 
						new DoubleWritable(Double.parseDouble(vals[1])));
			} else {
				labels.put(new Text(vals[0]), 
						new DoubleWritable(Double.parseDouble(vals[1])));
			}
		}
		//send all nodes to reducer to handle finish
		NodeLabelWritable current = new NodeLabelWritable();
		current.setAdjNodes(adjNodes);
		current.setLabels(labels);
		context.write(nodeID, current);
	}
}






