package friendRecMR;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class FriendRecIterReducer extends 
Reducer<Text, NodeLabelWritable, Text, NodeLabelWritable>{
	
	public FriendRecIterReducer() {
		super();
	}
	
	@Override
	public void reduce(Text nodeID, Iterable<NodeLabelWritable> iter, 
			Context context) throws IOException, InterruptedException {
		
		//Text -> DoubleWritable
		MapWritable labelMap = new MapWritable();
		//Text -> DoubleWritable
		MapWritable adjNodes = new MapWritable();
		
		for (NodeLabelWritable adj : iter) {
			//sum weights for each label
			for (Entry<Writable, Writable> label : adj.getLabels().entrySet()) {
				Text labelName = (Text) label.getKey();
				double labelWeight = ((DoubleWritable) label.getValue()).get();
				
				if (labelMap.containsKey(labelName)) {
					double w = ((DoubleWritable) labelMap.get(labelName)).get() 
							+ labelWeight;
					labelMap.put(labelName, new DoubleWritable(w));
				} else {
					labelMap.put(labelName, new DoubleWritable(labelWeight));
				}
			}
			
			//should only have 1
			for (Entry<Writable, Writable> node 
					: adj.getAdjNodes().entrySet()) {
				adjNodes.put(node.getKey(), node.getValue());
			}
		}
		
		//emit nodeID -> adj nodes and labels
		NodeLabelWritable currentNode = new NodeLabelWritable();
		currentNode.setLabels(labelMap);
		currentNode.setAdjNodes(adjNodes);
		context.write(nodeID, currentNode);
	}
	
}
