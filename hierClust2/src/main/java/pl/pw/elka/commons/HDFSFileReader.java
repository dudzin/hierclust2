package pl.pw.elka.commons;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class HDFSFileReader {

	/*
	 * public ArrayList<String> readFromFileByLine(String path, boolean trim) {
	 * ArrayList<String> array = new ArrayList<String>(); ; try {
	 * 
	 * FileSystem fs = FileSystem.get(new Configuration()); BufferedReader br =
	 * new BufferedReader(new InputStreamReader( fs.open(new Path(path))));
	 * String line; line = br.readLine(); while (line != null) { if (trim) {
	 * array.add(line.trim()); } else { array.add(line); } line = br.readLine();
	 * } } catch (Exception e) {
	 * 
	 * } return array; }
	 */

	public ArrayList<String> readFromFileByDelim(String path, String delim,
			boolean trim) {
		ArrayList<String> array = new ArrayList<String>();

		File file = new File(path);
		if (file.isFile()) {
			array = readSingleFile(file, delim, trim);
		} else {
			System.out.println("ffaaf:" + file);
			File[] files = file.listFiles();
			if (!(files == null)) {
				for (File f : files) {
					System.out.println("fff:" + f + "  asd " + file);
					if (f.isFile()) {
						System.out.println("fff:" + f + "  asd " + file);
						array.addAll(readSingleFile(f, delim, trim));
					}
				}
			}
		}
		return array;
	}

	public int readLineNumbers(String path, String delim, boolean trim,
			Configuration conf) {
		// Path loc = new Path(path);

		try {
			FileSystem fs = FileSystem.get(conf);
			Path filenamePath = new Path(path);
			int num = -1;
			if (!fs.exists(filenamePath)) {
				return -3;
			}

			if (fs.isDirectory(filenamePath)) {

				ArrayList<String> files = readFilesInDir(filenamePath, conf);
				if (files.size() != 1) {
					return -2;
				} else {
					Path f = new Path(path + "/" + files.get(0));
					FSDataInputStream fout = fs.open(f);

					String msgIn;
					num = 0;
					msgIn = fout.readLine();
					while (msgIn != null) {
						if (!msgIn.equals("")) {
							num++;
						}
						msgIn = fout.readLine();
					}
					// Print to screen
					fout.close();
					return num;
				}
			}
			// FSInputStream to read out of the filenamePath file
			return num;
		} catch (IOException e) {
			System.err.println("IOException during operation " + e.toString());
			System.exit(1);
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;
	}

	private ArrayList<String> readSingleFile(File file, String delim,
			boolean trim) {
		ArrayList<String> array = new ArrayList<String>();
		BufferedReader br = null;
		try {
			if (!file.getName().startsWith(".")) {
				String sCurrentLine;
				br = new BufferedReader(new FileReader(file.getPath()));

				while ((sCurrentLine = br.readLine()) != null) {
					sCurrentLine = sCurrentLine.trim();
					if (!delim.equals("\n")) {
						String[] sp = sCurrentLine.split(delim);
						for (String s : sp) {
							array.add(s.trim());
						}
					} else {
						if (trim) {
							array.add(sCurrentLine.trim());
						} else {
							array.add(sCurrentLine);
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return array;
	}

	/*
	 * public ArrayList<String> readFromFilesInDir(String path) {
	 * ArrayList<String> array = new ArrayList<String>(); BufferedReader br =
	 * null; try { File folder = new File(path); File[] listOfFiles =
	 * folder.listFiles(); for (File file : listOfFiles) { if
	 * (!file.getName().startsWith(".")) { String sCurrentLine;
	 * 
	 * br = new BufferedReader(new FileReader(file.getPath()));
	 * 
	 * while ((sCurrentLine = br.readLine()) != null) { sCurrentLine =
	 * sCurrentLine.trim(); String[] sp = sCurrentLine.split(","); for (String s
	 * : sp) { array.add(s.trim()); }
	 * 
	 * } } }
	 * 
	 * } catch (IOException e) { e.printStackTrace(); } finally { try { if (br
	 * != null) br.close(); } catch (IOException ex) { ex.printStackTrace(); } }
	 * return array; }
	 */

	public ArrayList<String> readFileNamesInDir(String path) {
		// System.out.println("path " + path);
		ArrayList<String> array = new ArrayList<String>();

		File folder = new File(path);
		File[] listOfFiles = folder.listFiles();
		for (File file : listOfFiles) {
			array.add(file.getName());
		}
		return array;

	}

	public ArrayList<String> readFilesInDir(Path location, Configuration conf)
			throws Exception {
		FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
		FileStatus[] items = fileSystem.listStatus(location);
		if (items == null)
			return new ArrayList<String>();
		ArrayList<String> results = new ArrayList<String>();
		for (FileStatus item : items) {
			results.add(item.getPath().getName());
		}
		return results;
	}

	public void writeFile(Path location, Configuration conf,
			ArrayList<String> list) throws IOException {

		// System.out.println("list size " + list.size());
		FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
		if (fileSystem.exists(location)) {
			fileSystem.delete(location, true);
		}
		OutputStream os = fileSystem.create(location, new Progressable() {
			public void progress() {
				// System.out.println("...bytes written: [ " + 22 + " ]");
			}
		});
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os,
				"UTF-8"));
		for (String string : list) {
			// System.out.println("write line: " + string);
			br.write(string);
			br.newLine();

		}
		br.close();
		fileSystem.close();
	}

	public boolean exists(String location, Configuration conf) {
		boolean exists = false;
		Path loc = new Path(location);
		try {
			if (!(location == null) && !location.equals("")) {
				FileSystem fileSystem;
				fileSystem = FileSystem.get(loc.toUri(), conf);
				if (fileSystem.exists(loc)) {
					exists = true;

				}
			}
		} catch (IOException e) {

		}
		return exists;
	}

	public boolean isDir(String location, Configuration conf) {
		Path loc = new Path(location);
		if (exists(location, conf)) {
			try {
				FileSystem fileSystem;
				fileSystem = FileSystem.get(loc.toUri(), conf);
				if (fileSystem.isDirectory(loc)) {
					return true;
				}
			} catch (IOException e) {
				return false;
			}
		}
		return false;
	}

	public boolean makeDir(String location, Configuration conf) {
		Path loc = new Path(location);
		if (!exists(location, conf)) {
			try {
				FileSystem fileSystem;
				fileSystem = FileSystem.get(loc.toUri(), conf);
				FsPermission p = new FsPermission(FsAction.ALL, FsAction.ALL,
						FsAction.ALL);
				fileSystem.mkdirs(loc, p);
				return true;
			} catch (IOException e) {
				return false;
			}
		}
		return false;
	}

	public boolean moveFile(String src, String dst, String pattern,
			Configuration conf) {
		try {
			FileSystem fileSystem;
			fileSystem = FileSystem.get(new Path(src).toUri(), conf);
			Path srcDir = new Path(src);

			ArrayList<String> files = readFilesInDir(srcDir, conf);

			boolean ret = false;
			for (String file : files) {

				if (file.startsWith(pattern)) {
					ret = fileSystem.rename(new Path(src + "/" + file),
							new Path(dst));
					ret = true;
				}
			}

			return ret;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public boolean removeDir(String location, boolean recursive,
			Configuration conf) {
		try {
			Path loc = new Path(location);
			FileSystem fileSystem;
			fileSystem = FileSystem.get(loc.toUri(), conf);
			return fileSystem.delete(loc, recursive);
		} catch (IOException e) {
			return false;
		}
	}
}
