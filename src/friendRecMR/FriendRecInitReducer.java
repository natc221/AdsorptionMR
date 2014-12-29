package friendRecMR;

import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FriendRecInitReducer extends 
Reducer<Text, NodeLabelWritable, Text, NodeLabelWritable> {
	
	public FriendRecInitReducer() {
		super();
	}
	
	@Override
	public void reduce(Text nodeID, Iterable<NodeLabelWritable> iter, 
			Context context) throws IOException, InterruptedException {
		
		MapWritable adjNodes = new MapWritable();
		MapWritable labels = new MapWritable();
		
		//do not sum weights of labels since they should initialize to 1
		for (NodeLabelWritable x : iter) {
			adjNodes.putAll(x.getAdjNodes());
			labels.putAll(x.getLabels());
		}
		
		NodeLabelWritable current = new NodeLabelWritable();
		current.setAdjNodes(adjNodes);
		current.setLabels(labels);
		
		context.write(nodeID, current);
	}
	
}
