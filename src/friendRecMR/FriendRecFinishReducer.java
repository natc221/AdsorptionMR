package friendRecMR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class FriendRecFinishReducer extends
		Reducer<Text, NodeLabelWritable, Text, Text> {
	
	public static final int TOP_K_ELEMENTS = 10;
	
	public FriendRecFinishReducer() {
		super();
	}
	
	@Override
	public void reduce(Text nodeID, Iterable<NodeLabelWritable> iter, 
			Context context) throws IOException, InterruptedException {
		//should only have one
		NodeLabelWritable node = iter.iterator().next();
		Set<Writable> adjNodes = node.getAdjNodes().keySet();
		
		//max heap to store all labels
		PriorityQueue<Label> heap = new PriorityQueue<Label>();
		for (Entry<Writable, Writable> label : node.getLabels().entrySet()) {
			String l = label.getKey().toString();
			double w = ((DoubleWritable) label.getValue()).get();
			Label toAdd = new Label(l, w);
			heap.add(toAdd);
		}
		
		//get top k elements
		int noToCollect = TOP_K_ELEMENTS;
		//list so that it will be in order of highest label weight
		ArrayList<Label> topK = new ArrayList<Label>();
		while (!heap.isEmpty() && noToCollect > 0) {
			Label label = heap.poll();
			/*
			 * check that the label is not already an adjacent node, 
			 * and is not the node itself
			 */
			Text toCheck = new Text(label.label);
			if (!adjNodes.contains(toCheck) && !nodeID.equals(toCheck)) {
				topK.add(label);
				noToCollect--;
			}
		}
		
		//output top elements for current node
		String output = "";
		for (Label l : topK) {
			output += l.label + " ";
		}
		//nodeID -> labels collected
		context.write(nodeID, new Text(output));
	}
	
	private class Label implements Comparable<Label> {
		private String label;
		private double weight;
		
		public Label(String label, double weight) {
			this.label = label;
			this.weight = weight;
		}

		@Override
		public int compareTo(Label other) {
			if (this.weight < other.weight) {
				return 1;
			} else {
				return -1;
			}
		}
		
	}
}
