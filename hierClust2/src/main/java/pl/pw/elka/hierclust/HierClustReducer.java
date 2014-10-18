package pl.pw.elka.hierclust;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import pl.pw.elka.commons.DistMatrixRow;
import pl.pw.elka.commons.HDFSFileReader;

public class HierClustReducer extends Reducer<Text, Text, Text, Text> {

	private ArrayList<DistMatrixRow> rows;
	private ArrayList<String> dict;
	private MultipleOutputs<Text, Text> mos;
	private String option;
	private String clustFNames;
	private String connType;

	ArrayList<String> keys = new ArrayList<String>();
	ArrayList<Integer> mindists = new ArrayList<Integer>();
	ArrayList<String> vals = new ArrayList<String>();

	public void setup(Context context) throws IOException, InterruptedException {

		super.setup(context);
		rows = new ArrayList<DistMatrixRow>();
		option = context.getConfiguration().get("option");

		HDFSFileReader reader = new HDFSFileReader();
		String path = context.getConfiguration().get("dict");
		path = "." + path.substring(path.lastIndexOf("/"));

		dict = reader.readFromFileByDelim(path, ",", false);
		mos = new MultipleOutputs(context);

		// path = context.getConfiguration().get("clust");
		// path = "." + path.substring(path.lastIndexOf("/"));

		// ArrayList<String> tmp = reader.readFromFileByDelim(path, ",", true);
		// String[] sp;
		clustFNames = context.getConfiguration().get("output.clusterfilenames");
		connType = context.getConfiguration().get("connType");
		if (connType.equals("")) {
			connType = "CompleteLink";
		}
	}

	public void reduce(Text key, Iterable<Text> lines, Context context)
			throws IOException, InterruptedException {

		if (option.equals("p")) {
			reduceParallel(context, lines);
		} else if (option.equals("l")) {
			reduceLinear(context, lines);
		}

	}

	private void reduceParallel(Context context, Iterable<Text> lines) {

		rows = new ArrayList<DistMatrixRow>();
		/**
		 * Computational complexity: 2 or 4 A
		 */
		for (Text text : lines) {
			DistMatrixRow row = new DistMatrixRow(text);
			rows.add(row);
		}
		try {
			if (rows.size() == 2) {
				processPair(context);
			} else if (rows.size() == 1) {
				processSingle(context);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Computational complexity: Merge + getzeros + zeros*(A + getCluster) + 2W
	 */
	private void processPair(Context context) throws Exception {

		int mindist = rows.get(0).getMinDist();
		DistMatrixRow newRow = rows.get(0).merge(rows.get(1), connType);
		String zeros = newRow.getZerosPositions();
		String nk = "";
		String[] z = zeros.split(",");
		for (String s : z) {

			nk = nk + getCluster(Integer.parseInt(s)) + "&";
		}
		nk = nk.substring(0, nk.length() - 1);
		String newkey = nk;

		context.write(new Text(newkey), new Text("" + newRow));
		if (!clustFNames.equals("")) {

			mos.write(
					context.getConfiguration().get("output.clusterfilenames"),
					newkey, new Text("" + mindist));
		}
	}

	private void processSingle(Context context) throws Exception {
		DistMatrixRow newRow = rows.get(0);
		//
		processSingle(context, newRow);
	}

	private void processSingle(Context context, DistMatrixRow newRow)
			throws Exception {

		//
		String zeros = newRow.getZerosPositions();
		String nk = "";
		String[] z = zeros.split(",");
		for (String s : z) {

			nk = nk + getCluster(Integer.parseInt(s)) + "&";
		}
		nk = nk.substring(0, nk.length() - 1);
		// String newkey = "" + sp[0] + ":" + sp[1];
		String newkey = nk;
		//

		context.write(new Text(newkey), new Text("" + newRow));
		if (!clustFNames.equals("")) {
			mos.write(
					context.getConfiguration().get("output.clusterfilenames"),
					newkey, new Text("" + 0));
		}
	}

	/**
	 * Computational complexity: Cluster size * dict size
	 */

	private String getCluster(int pos) {

		String p = dict.get(pos);
		for (String s : dict) {
			if (s.contains(p)) {
				return s;
			}
		}
		return null;
	}

	private void reduceLinear(Context context, Iterable<Text> lines) {
		rows = new ArrayList<DistMatrixRow>();

		readLinesIntoArrays(lines);
		String[] toRemove = findPair();

		try {
			processPair(context);
		} catch (Exception e) {
			e.printStackTrace();
		}
		vals.remove(toRemove[0]);
		vals.remove(toRemove[1]);

		DistMatrixRow row;
		for (String val : vals) {
			row = new DistMatrixRow(new Text(val));

			try {
				processSingle(context, row);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	private void readLinesIntoArrays(Iterable<Text> lines) {
		keys = new ArrayList<String>();
		mindists = new ArrayList<Integer>();
		vals = new ArrayList<String>();

		String[] split;
		for (Text text : lines) {
			split = text.toString().split("@");
			keys.add(split[0]);
			mindists.add(Integer.parseInt(split[1]));
			vals.add(split[2]);

		}

	}

	private String[] findPair() {

		int min = Collections.min(mindists);
		int ind1 = 0, ind2 = 0;
		HashMap<String, Integer> minKeys = new HashMap<String, Integer>();
		for (int i = 0; i < mindists.size(); i++) {
			if (minKeys.containsKey(keys.get(i))) {
				ind1 = minKeys.get(keys.get(i));
				ind2 = i;
				break;
			} else if (mindists.get(i) == min) {
				minKeys.put(keys.get(i), i);
			}
		}

		rows = new ArrayList<DistMatrixRow>();
		String val1 = vals.get(ind1);
		DistMatrixRow row = new DistMatrixRow(new Text(val1));
		rows.add(row);
		String val2 = vals.get(ind2);
		row = new DistMatrixRow(new Text(val2));
		rows.add(row);

		String[] ar = { val1, val2 };
		return ar;
	}

	public void cleanup(Context context) throws IOException {
		try {
			mos.close();
			super.cleanup(context);

		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private String generateFileName(Text k, Text v) {
		return k.toString() + "_" + v.toString();
	}
}
