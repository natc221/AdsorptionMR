package friendRecMR;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriendRecIterMapper extends 
Mapper<LongWritable, Text, Text, NodeLabelWritable> {
	
	public FriendRecIterMapper() {
		super();
	}
	
	@Override
	public void map(LongWritable key, Text val, Context context) 
	throws IOException, InterruptedException {
		String line = val.toString();
		String[] split = line.split("\\s+");
		
		Text nodeID = new Text(split[0]);

		Map<String, Double> adjNodes = new HashMap<String, Double>();
		Map<String, Double> labels = new HashMap<String, Double>();
		
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
				adjNodes.put(vals[0], Double.parseDouble(vals[1]));
			} else {
				labels.put(vals[0], Double.parseDouble(vals[1]));
			}
		}
		
		//propogate labels to each adjacent node
		for (Entry<String, Double> adj : adjNodes.entrySet()) {
			Text adjID = new Text(adj.getKey());
			double edgeWeight = adj.getValue();
			
			//calculate new label weights
			MapWritable newLabelMap = new MapWritable();
			for (Entry<String, Double> label : labels.entrySet()) {
				DoubleWritable newWeight =  
						new DoubleWritable(label.getValue() * edgeWeight);
				newLabelMap.put(new Text(label.getKey()), newWeight);
			}
			NodeLabelWritable adjNode = new NodeLabelWritable();
			adjNode.setLabels(newLabelMap);
			context.write(adjID, adjNode);
			
			//preserve current edges
			NodeLabelWritable current = new NodeLabelWritable();
			current.addAdjacent(adjID, edgeWeight);
			context.write(nodeID, current);
		}
	}
}
