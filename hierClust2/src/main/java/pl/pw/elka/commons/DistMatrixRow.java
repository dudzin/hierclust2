package pl.pw.elka.commons;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;

import pl.pw.elka.hierclust.HierClustDriver;

public class DistMatrixRow {

	private int[] distances;
	private int rowPos;
	private Context c;

	public DistMatrixRow(int length) {
		distances = new int[length];
	}

	public DistMatrixRow(Text text, Context c) {

		String[] s = text.toString().split(",");

		distances = new int[s.length];
		this.c = c;
		for (int i = 0; i < s.length; i++) {
			distances[i] = Integer.parseInt(s[i]);
			if (distances[i] == 0) {
				rowPos = i;
			}
		}
	}

	/**
	 * Computational complexity: A + IF + dict size * (IF + A) + IF + A
	 */

	public DistMatrixRow merge(DistMatrixRow other, String connType)
			throws Exception {

		DistMatrixRow newRow = new DistMatrixRow(this.distances.length);

		if (this.getDistances().length != other.getDistances().length) {
			throw new java.lang.Exception(
					"Merge Exception: DistMatrixRow not of the same size");
		}

		int[] newDistances = new int[distances.length];
		if (connType.equals("SingleLink")) {
			for (int i = 0; i < distances.length; i++) {
				if (this.getDistances()[i] < other.getDistances()[i]) {
					newDistances[i] = this.getDistances()[i];
					assIncr();
				} else {
					newDistances[i] = other.getDistances()[i];
					assIncr();
				}
				ifIncr();
			}
		} else if (connType.equals("CompleteLink")) {
			for (int i = 0; i < distances.length; i++) {
				if (this.getDistances()[i] == 0 || other.getDistances()[i] == 0) {
					newDistances[i] = 0;
					assIncr();
				} else if (this.getDistances()[i] >= other.getDistances()[i]) {
					newDistances[i] = this.getDistances()[i];
					assIncr();
				} else {
					newDistances[i] = other.getDistances()[i];
					assIncr();
				}
				ifIncr();
			}
		}
		ifIncr();
		newRow.setDistances(newDistances);
		return newRow;

	}

	public int getRowPos() {
		return rowPos;
	}

	public int[] getDistances() {
		return distances;
	}

	public void setDistances(int[] distances) {
		this.distances = distances;
	}

	public String toString() {
		String s = "";

		for (int i : distances) {
			s += i + ",";
		}
		s = s.substring(0, s.length() - 1);

		return s;

	}

	/**
	 * Computational complexity: dict size * (IF + A) + A
	 */
	public String getZerosPositions() {
		String s = "";
		for (int i = 0; i < distances.length; i++) {
			if (distances[i] == 0) {
				s += i + "&";
			}
			ifIncr();
		}
		s = s.substring(0, s.length() - 1);

		return s;

	}

	/**
	 * Computational complexity: dict * (2IF + A)
	 */
	public int getMinDist() {
		int min = -1;
		for (int i : distances) {
			if (i == 0) {
			} else if (min == -1) {
				min = i;
				assIncr();
			} else {
				if (min > i) {
					min = i;
					assIncr();
				}
			}
			ifIncr();
		}
		return min;
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
