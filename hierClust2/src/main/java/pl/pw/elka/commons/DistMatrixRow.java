package pl.pw.elka.commons;

import org.apache.hadoop.io.Text;

public class DistMatrixRow {

	private int[] distances;
	private int rowPos;

	public DistMatrixRow(int length) {
		distances = new int[length];
	}

	public DistMatrixRow(Text text) {

		String[] s = text.toString().split(",");

		distances = new int[s.length];

		for (int i = 0; i < s.length; i++) {
			distances[i] = Integer.parseInt(s[i]);
			if (distances[i] == 0) {
				rowPos = i;
			}
		}
	}

	/**
	 * Computational complexity: dict size * (IF + A) + IF
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
				} else {
					newDistances[i] = other.getDistances()[i];
				}
			}
		} else // if (connType.equals("CompleteLink"))
		{
			for (int i = 0; i < distances.length; i++) {
				if (this.getDistances()[i] == 0 || other.getDistances()[i] == 0) {
					newDistances[i] = 0;
				} else if (this.getDistances()[i] >= other.getDistances()[i]) {
					newDistances[i] = this.getDistances()[i];
				} else {
					newDistances[i] = other.getDistances()[i];
				}
			}
		}
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
				s += i + ",";
			}
		}
		s = s.substring(0, s.length() - 1);

		return s;

	}

	public int getMinDist() {
		int min = -1;
		for (int i : distances) {
			if (i == 0) {
			} else if (min == -1) {
				min = i;

			} else {
				if (min > i) {
					min = i;
				}
			}
		}
		return min;
	}
}
