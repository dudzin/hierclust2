package pl.pw.elka.hierclust;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pl.pw.elka.commons.DistMatrixRow;
import pl.pw.elka.commons.HDFSFileReader;

public class HierClustMapper extends Mapper<LongWritable, Text, Text, Text> {

	int minval;
	private ArrayList<String> dict;
	// private ArrayList<String> clust;
	private ArrayList<Cluster> clust;
	private DistMatrixRow distrow;
	private String option;
	private ArrayList<Cluster> changed;
	private String connType;
	private Context c;

	char CLUSTJOIN = '&';

	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		c = context;
		option = c.getConfiguration().get("option");

		HDFSFileReader reader = new HDFSFileReader();

		String path = context.getConfiguration().get("dict");
		path = "." + path.substring(path.lastIndexOf("/"));

		dict = reader.readFromFileByDelim(path, ",", false);

		path = context.getConfiguration().get("clust");
		path = "." + path.substring(path.lastIndexOf("/"));

		connType = context.getConfiguration().get("connType");
		if (connType.equals("")) {
			connType = "CompleteLink";
		}

		ArrayList<String> tmp = reader.readFromFileByDelim(path, ",", true);
		String[] sp;
		clust = new ArrayList<HierClustMapper.Cluster>();
		changed = new ArrayList<HierClustMapper.Cluster>();
		for (String t : tmp) {
			sp = t.split("\t");
			if (sp.length == 2) {
				Cluster c = new Cluster(sp[0], Integer.parseInt(sp[1]));
				clust.add(c);
				if (c.getVal() != 0) {
					changed.add(c);
					System.out.println("clust: " + c.getName() + " "
							+ c.getVal());
				}
			} else {
				clust.add(new Cluster(sp[0], 0));
			}
		}

		/*
		 * changed = new ArrayList<HierClustMapper.Cluster>(); for (Cluster
		 * cluster : clust) { if (cluster.getVal() != 0) { changed.add(cluster);
		 * System.out.println("clust: " + cluster.getName() + " " +
		 * cluster.getVal()); } }
		 */
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] s = value.toString().split("	");
		String distText = s[1];

		String distKey = s[0];// .replace(":", ",");

		int smallestPos = -1;
		/**
		 * Computational complexity: recalcRow
		 */

		distText = recalcRow(distText);
		String[] distances = distText.split(",");
		/**
		 * Computational complexity: findSMallestDist
		 */
		smallestPos = findSmallestDist(distances);
		String outkey = "";
		/**
		 * Computational complexity: if + A + getOutputKey + W
		 */

		if (option.equals("p")) {

			distrow = new DistMatrixRow(new Text(distText), null);
			// outkey = getOutputKey(distrow.getZerosPositions() + ","
			outkey = getOutputKey(distKey, smallestPos);

			context.write(new Text(outkey), new Text(distText));
		} else if (option.equals("l")) {
			outkey = "1";
			distrow = new DistMatrixRow(new Text(distText), null);
			// String skey = getOutputKey(distrow.getZerosPositions() + ","
			String skey = getOutputKey(distKey, smallestPos);

			context.write(new Text(outkey), new Text(skey + "@"
					+ distances[smallestPos] + "@" + distText));
		}
	}

	/**
	 * Computational complexity: 3A + dict *(2A + 2IF )
	 */

	private int findSmallestDist(String[] distances) {
		// int rowPos = -1;
		// int pos;
		int smallestPos = -1;
		int smallestVal = -1;
		int dist = -1;

		/*
		 * for (int i = 0; i < distances.length; i++) { pos =
		 * Integer.parseInt(distances[i]); dist =
		 * Integer.parseInt(distances[i]); if (pos == 0) { } else { if
		 * (smallestPos == -1) {
		 * 
		 * if (rowPos != i) { smallestPos = i; } } else if
		 * (Integer.parseInt(distances[smallestPos]) > Integer
		 * .parseInt(distances[i])) { smallestPos = i; } } }
		 */

		for (int i = 0; i < distances.length; i++) {
			// pos = Integer.parseInt(distances[i]);
			dist = Integer.parseInt(distances[i]);

			if (dist == 0) {

			} else {
				if (smallestPos == -1) {
					smallestPos = i;
					smallestVal = dist;
					assIncr(2);
				} else if (smallestVal > dist) {
					smallestPos = i;
					smallestVal = dist;
					assIncr(2);
				}
				ifIncr();
			}
			ifIncr();
		}
		return smallestPos;
	}

	/**
	 * Computational complexity: ci * CS* IF
	 */
	private String getClusterName(int pos) {

		String p = "" + pos;
		String[] sp;
		for (Cluster c : clust) {
			sp = c.getName().split("" + CLUSTJOIN);
			assIncr();
			for (int i = 0; i < sp.length; i++) {
				if (sp[i].equals(p)) {// c.getName().contains(p)) {
					return c.getName();
				}
				ifIncr();
			}
		}
		return null;
	}

	/**
	 * New: getClusterName + [CS+1]*A + [CS+1]*A =
	 * 
	 * = ci*IF + 2A(CS + 1)
	 */

	private String getOutputKey(String incluster, int smallest) {

		String newkey = "";
		Set<Integer> clusterPositions = new TreeSet<Integer>();

		incluster += CLUSTJOIN + getClusterName(smallest);
		String[] clustParts = incluster.split("" + CLUSTJOIN);
		for (String part : clustParts) {
			assIncr();
			clusterPositions.add(Integer.parseInt(part));
		}

		for (Integer pos : clusterPositions) {
			assIncr();
			newkey = newkey + pos + CLUSTJOIN;
		}
		return newkey.substring(0, newkey.length() - 1);

	}

	public void setDict(ArrayList<String> list) {
		this.dict = list;
	}

	/**
	 * Computational complexity: Clusters list size * if
	 */
	public void setClust(ArrayList<String> list) {
		clust = new ArrayList<HierClustMapper.Cluster>();
		String[] sp;
		changed = new ArrayList<HierClustMapper.Cluster>();
		Cluster cluster;
		for (String t : list) {
			sp = t.split("\t");
			cluster = new Cluster(sp[0], Integer.parseInt(sp[1]));
			clust.add(cluster);
			assIncr(3);
			if (cluster.getVal() != 0) {
				changed.add(cluster);
				System.out.println("clust: " + cluster.getName() + " "
						+ cluster.getVal());
				assIncr();
			}
			ifIncr();
		}

	}

	/**
	 * Computational complexity:
	 */

	public String recalcRow(String row) {

		int[] newrow = initNewRow(row);
		newrow = applyChanges(newrow);
		return newRowToString(newrow);
	}

	/**
	 * Computational complexity: dict* A
	 * 
	 * */
	private int[] initNewRow(String row) {
		String[] sp = row.split(",");
		int[] newrow = new int[sp.length];
		for (int i = 0; i < newrow.length; i++) {
			newrow[i] = Integer.parseInt(sp[i]);
			assIncr();
		}
		return newrow;
	}

	/**
	 * Computational complexity: [C(i-1) - C(i)]* ( 2A + findVal + CS*A)
	 * 
	 * */

	private int[] applyChanges(int[] newrow) {
		// int[] posToChange;
		String[] clustpos;
		int val;

		for (Cluster cluster : changed) {
			clustpos = cluster.getName().split("" + CLUSTJOIN);
			// posToChange = new int[clustpos.length];
			val = findVal(clustpos, newrow);// , posToChange);

			assIncr(2);
			for (String i : clustpos) { // posToChange) {
				newrow[Integer.parseInt(i)] = val;
				assIncr();
			}
		}
		return newrow;
	}

	/**
	 * Computational complexity: IF + CS*(2IF + A)
	 */
	private int findVal(String clustpos[], int[] newrow// , int[] posToChange
	) {

		int val = -1;
		if (connType.equals("CompleteLink")) {
			for (int i = 0; i < clustpos.length; i++) {
				// posToChange[i] = Integer.parseInt(clustpos[i]);
				if (val == -1) {
					// val = newrow[posToChange[i]];
					val = newrow[Integer.parseInt(clustpos[i])];
					assIncr();
				} else {
					// if (val < newrow[posToChange[i]]) {
					// val = newrow[posToChange[i]];
					// }
					if (val < newrow[Integer.parseInt(clustpos[i])]) {
						val = newrow[Integer.parseInt(clustpos[i])];
						assIncr();
					}
					ifIncr();
				}
				ifIncr();
			}
		} else {
			for (int i = 0; i < clustpos.length; i++) {
				// posToChange[i] = Integer.parseInt(clustpos[i]);
				if (val == -1) {
					val = newrow[Integer.parseInt(clustpos[i])];
					assIncr();
				} else {
					// if (val > newrow[posToChange[i]]) {
					// val = newrow[posToChange[i]];
					// }
					if (val > newrow[Integer.parseInt(clustpos[i])]) {
						val = newrow[Integer.parseInt(clustpos[i])];
						assIncr();
					}
					ifIncr();
				}
				ifIncr();
			}
		}
		ifIncr();
		return val;
	}

	/**
	 * Computational complexity: dict size * assign
	 */
	private String newRowToString(int[] newrow) {
		String retrow = "";
		for (int i = 0; i < newrow.length; i++) {
			retrow += newrow[i] + ",";
			assIncr();
		}
		retrow = retrow.substring(0, retrow.length() - 1);
		assIncr();
		return retrow;
	}

	private class Cluster {
		private String name;
		private int val;

		public Cluster(String name, int val) {
			this.name = name;
			this.val = val;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public void setVal(int val) {
			this.val = val;
		}

		public int getVal() {
			return val;
		}
	}

	public void setConnType(String connType) {
		this.connType = connType;
	}

	private void ifIncr() {
		c.getCounter(HierClustDriver.COMPLEXITY_COUNTER.IF_CNT).increment(1);
	}

	private void ifIncr(int i) {
		c.getCounter(HierClustDriver.COMPLEXITY_COUNTER.IF_CNT).increment(i);
	}

	private void assIncr() {
		c.getCounter(HierClustDriver.COMPLEXITY_COUNTER.ASS_CNT).increment(1);
	}

	private void assIncr(int i) {
		c.getCounter(HierClustDriver.COMPLEXITY_COUNTER.ASS_CNT).increment(i);
	}

}
