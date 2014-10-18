package pl.pw.elka.hierclust;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
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

	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		option = context.getConfiguration().get("option");

		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());

		for (Path path : cacheFiles) {
			System.out.println("dist file: " + path);
		}

		HDFSFileReader reader = new HDFSFileReader();

		String path = context.getConfiguration().get("dict");
		path = "." + path.substring(path.lastIndexOf("/"));

		System.out.println("path " + path);
		// dict = reader.readFileIntoArray(path);
		dict = reader.readFromFileByDelim(path, ",", false);

		path = context.getConfiguration().get("clust");
		path = "." + path.substring(path.lastIndexOf("/"));

		System.out.println("path " + path);

		connType = context.getConfiguration().get("connType");
		if (connType.equals("")) {
			connType = "CompleteLink";
		}

		ArrayList<String> tmp = reader.readFromFileByDelim(path, ",", true);
		String[] sp;
		// clust = new ArrayList<String>();
		// clust = new LinkedHashMap<String, Integer>();
		clust = new ArrayList<HierClustMapper.Cluster>();
		for (String t : tmp) {
			sp = t.split("\t");
			if (sp.length == 2) {
				clust.add(new Cluster(sp[0], Integer.parseInt(sp[1])));
			} else {
				clust.add(new Cluster(sp[0], 0));
			}
		}

		changed = new ArrayList<HierClustMapper.Cluster>();
		for (Cluster cluster : clust) {
			if (cluster.getVal() != 0) {
				changed.add(cluster);
				System.out.println("clust: " + cluster.getName() + " "
						+ cluster.getVal());
			}
		}
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] s = value.toString().split("	");
		String distText = s[1];
		distText = recalcRow(distText);

		String[] distances = distText.split(",");

		int rowPos = -1;
		int smallestPos = -1;
		int pos;

		/**
		 * Computational complexity: dict size * (if*2 + assign)
		 */
		for (int i = 0; i < distances.length; i++) {
			pos = Integer.parseInt(distances[i]);
			if (pos == 0) {
			} else {
				if (smallestPos == -1) {

					if (rowPos != i) {
						smallestPos = i;
					}
				} else if (Integer.parseInt(distances[smallestPos]) > Integer
						.parseInt(distances[i])) {
					smallestPos = i;
				}
			}
		}

		String outkey = "";
		/**
		 * Computational complexity: if + dict size * (if + assign) +
		 * getOutputKey
		 */
		if (option.equals("p")) {

			distrow = new DistMatrixRow(new Text(distText));
			outkey = getOutputKey(distrow.getZerosPositions() + ","
					+ smallestPos);

			context.write(new Text(outkey), new Text(distText));
		} else if (option.equals("l")) {
			outkey = "1";
			distrow = new DistMatrixRow(new Text(distText));
			String skey = getOutputKey(distrow.getZerosPositions() + ","
					+ smallestPos);

			context.write(new Text(outkey), new Text(skey + "@"
					+ distances[smallestPos] + "@" + distText));
		}
	}

	/**
	 * Computational complexity: Cluster size * IF
	 */
	private String getClusterName(int pos) {

		String p = dict.get(pos);
		for (Cluster c : clust) {
			if (c.getName().contains(p)) {
				return c.getName();
			}
		}
		return null;
	}

	/**
	 * Computational complexity: Cluster size * (getClusterName + getClusterPos
	 * + clustersize* assign)
	 */

	private String getOutputKey(String zerospos) {

		String newkey = "";
		String[] zeros = zerospos.split(",");
		Set<Integer> posSet = new TreeSet<Integer>();
		String clusterfull;
		String clusterpos;

		for (String i : zeros) {
			clusterfull = getClusterName(Integer.parseInt(i));
			clusterpos = getClusterPos(clusterfull);
			String[] sp = clusterpos.split(",");
			for (String string : sp) {
				posSet.add(Integer.parseInt(string));
			}

		}

		for (Integer i : posSet) {
			newkey = newkey + i + ",";
		}
		return newkey.substring(0, newkey.length() - 1);

	}

	/**
	 * Computational complexity: Cluster size * dict size * if
	 */
	public String getClusterPos(String ss) {
		String pos = "";
		String[] s = ss.split("&");
		for (String string : s) {
			for (int i = 0; i < dict.size(); i++) {
				if (dict.get(i).equals(string)) {
					pos = pos + i + ",";
				}
			}
		}

		return pos.substring(0, pos.length() - 1);
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
			if (cluster.getVal() != 0) {
				changed.add(cluster);
				System.out.println("clust: " + cluster.getName() + " "
						+ cluster.getVal());
			}
		}

	}

	/**
	 * Computational complexity: dict size * assign + number of new clusters * (
	 * getClusterPosComplexity + findValComplex + assign + size of cluster *
	 * assign) + createNewRow
	 */

	public String recalcRow(String row) {

		String[] sp = row.split(",");
		int[] newrow = new int[sp.length];
		for (int i = 0; i < newrow.length; i++) {
			newrow[i] = Integer.parseInt(sp[i]);
		}

		int[] posToChange;
		String[] clustpos;
		int val;

		for (Cluster cluster : changed) {
			clustpos = getClusterPos(cluster.getName()).split(",");
			posToChange = new int[clustpos.length];
			val = findVal(clustpos, newrow, posToChange);
			for (int i : posToChange) {
				newrow[i] = val;
			}
		}
		String retrow = createNewRow(newrow);
		System.out.println(retrow);
		return retrow;
	}

	/**
	 * Computational complexity: Clusterpos size + if + assign
	 */
	private int findVal(String clustpos[], int[] newrow, int[] posToChange) {

		int val = -1;
		if (connType.equals("CompleteLink")) {
			for (int i = 0; i < clustpos.length; i++) {
				posToChange[i] = Integer.parseInt(clustpos[i]);
				if (val == -1) {
					val = newrow[posToChange[i]];
				} else {
					if (val < newrow[posToChange[i]]) {
						val = newrow[posToChange[i]];
					}
				}
			}
		} else {
			for (int i = 0; i < clustpos.length; i++) {
				posToChange[i] = Integer.parseInt(clustpos[i]);
				if (val == -1) {
					val = newrow[posToChange[i]];
				} else {
					if (val > newrow[posToChange[i]]) {
						val = newrow[posToChange[i]];
					}
				}
			}
		}
		return val;
	}

	/**
	 * Computational complexity: dict size * assign
	 */
	private String createNewRow(int[] newrow) {
		String retrow = "";
		for (int i = 0; i < newrow.length; i++) {
			retrow += newrow[i] + ",";
		}
		retrow = retrow.substring(0, retrow.length() - 1);
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
}
