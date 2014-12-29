package friendRecMR;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class NodeLabelWritable implements Writable {

	//Text -> DoubleWritable
	private MapWritable adjNodes;
	//Text -> DoubleWritable
	private MapWritable labels;

	public NodeLabelWritable() {
		adjNodes = new MapWritable();
		labels = new MapWritable();
	}

	public void addAdjacent(Text nodeID, double edgeWeight) {
		adjNodes.put(nodeID, new DoubleWritable(edgeWeight));
	}

	public void addLabel(Text label, double weight) {
		labels.put(label, new DoubleWritable(weight));
	}
	
	public void setAdjNodes(MapWritable adjNodes) {
		this.adjNodes = adjNodes;
	}
	
	public void setLabels(MapWritable labels) {
		this.labels = labels;
	}
	
	public MapWritable getAdjNodes() {
		return adjNodes;
	}
	
	public MapWritable getLabels() {
		return labels;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		adjNodes = new MapWritable();
		adjNodes.readFields(arg0);
		labels = new MapWritable();
		labels.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		adjNodes.write(arg0);
		labels.write(arg0);
	}
	
	@Override
	public String toString() {
		String s = "adj: ";
		for (Entry<Writable, Writable> entry : adjNodes.entrySet()) {
			s += "(" + entry.getKey() + "," + entry.getValue() + ") ";
		}
		s += " labels: ";
		for (Entry<Writable, Writable> entry : labels.entrySet()) {
			s += "(" + entry.getKey() + "," + entry.getValue() + ") ";
		}
		return s;
	}
	
}
