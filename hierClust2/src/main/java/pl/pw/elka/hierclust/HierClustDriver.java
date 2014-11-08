package pl.pw.elka.hierclust;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import pl.pw.elka.commons.HDFSFileReader;

public class HierClustDriver extends Configured implements Tool {
	HDFSFileReader hdfs;
	ArrayList<Long> jobtimes;
	long t;
	private long ifCnt, assCnt;

	public static enum COMPLEXITY_COUNTER {
		IF_CNT, ASS_CNT;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new HierClustDriver(),
				args);
		System.exit(res);

	}

	// sudo -u hdfs hadoop jar hierclust-0.0.3-SNAPSHOT.jar
	// pl.pw.elka.hierclust.HierClustDriver -D input="hierclust2/input" -D
	// output="hierclust2/output" -D dict="hierclust2/dictionary" -D
	// clust="hierclust2/clusters"

	// sudo -u hdfs hadoop jar hierclust-0.0.3-SNAPSHOT.jar
	// pl.pw.elka.hierclust.HierClustDriver -D input="hierclust2/input" -D
	// dict="hierclust2/dictionary" -D clust="hierclust2/clusters"

	// TO-DO
	/*
	 * To-Do: 4. nie ma potrzeby przechowywania informacji A 0,2,3. zera
	 * wystarczą. można dodać jeżeli napewno dictionary zawsze tworzy się w
	 * odpowiedniej kolejnosci - np najpeirw tworzony jest słownik, a potem
	 * wszystkie wpisy w distmx są do niego dostosowane 5. dodać dist połączenia
	 * w clusters
	 */

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();
		hdfs = new HDFSFileReader();

		if (!checkParams()) {
			System.out.println("incorrect input parameters");
			return 1;
		}

		boolean jobCompletion = true;
		int stepResult = 2;
		int endCond = conf.getInt("endcond", 0);
		int step;
		ifCnt = 0;
		assCnt = 0;
		confPropertiesInit();
		step = conf.getInt("step", 0);
		// long startTime = System.nanoTime();
		ArrayList<Long> times = new ArrayList<Long>();
		jobtimes = new ArrayList<Long>();
		times.add(System.nanoTime());
		while (stepResult > endCond && jobCompletion) {

			jobCompletion = runStep(step);
			String clustPath = moveClusterInfo(step);
			if (clustPath != null) {
				stepResult = this.hdfs.readLineNumbers(clustPath, "\n", false,
						conf);
			} else {
				stepResult = -5;
			}

			System.out.println("output number of clusters is: " + stepResult);
			step = confPropertiesIncrement();
			times.add(System.nanoTime());
		}

		printExecutionTimes(times);
		return 0; // exec ? 0 : 1;
	}

	private void printExecutionTimes(ArrayList<Long> times) {
		System.out.println("Execution time for steps: ");
		for (int i = 1; i < times.size(); i++) {
			System.out.println("Step " + i + ": "
					+ (times.get(i) - times.get(i - 1)) / 1000000000 + " sec");
		}

		long fulltime = 0;
		for (int i = 1; i < jobtimes.size(); i++) {
			System.out.println("Step " + i + " job time: " + (jobtimes.get(i))
					/ 1000000000 + " sec");
			fulltime += (jobtimes.get(i)) / 1000000000;
		}

		System.out.println("\nTotal Execution time: "
				+ (times.get(times.size() - 1) - times.get(0)) / 1000000000);

		System.out.println("Counters: ");
		System.out.println("IF	:	" + ifCnt);
		System.out.println("ASS	:	" + assCnt);

		// System.out.println("\nTotal Job Execution time: " + fulltime);
	}

	private boolean checkParams() {

		Configuration conf = this.getConf();

		System.out.println("input params : ");
		String input = conf.get("input");
		String dict = conf.get("dict");
		String clust = conf.get("clust");
		String option = conf.get("option");
		int step = conf.getInt("step", 0);
		conf.setInt("endcond", conf.getInt("endcond", 1));
		int endcond = conf.getInt("endcond", -1);
		String connType = conf.get("connType");

		System.out.println("input:     " + input);
		System.out.println("dict:      " + dict);
		System.out.println("clust:     " + clust);
		System.out.println("step:      " + step);
		System.out.println("endcond:   " + endcond);
		System.out.println("option:    " + option);
		System.out.println("connType:  " + connType);

		boolean launch = true;
		if (!hdfs.exists(input, conf)) {
			System.out.println("input directory does not exist");
			launch = false;
		} else if (!hdfs.isDir(input, conf)) {
			System.out.println("input is not a directory");
			launch = false;
		}

		if (!hdfs.exists(dict, conf)) {
			System.out.println("dictionary file does not exist");
			launch = false;
		} else if (hdfs.isDir(dict, conf)) {
			System.out.println("dictionary is not a file");
			launch = false;
		}

		if (!hdfs.exists(clust, conf)) {
			System.out.println("clust directory does not exist");
			if (hdfs.makeDir(clust, conf)) {
				System.out.println("clust directory created");
				launch = true;
			} else {
				System.out.println("failed to create clust directory");
				return false;
			}
		} else if (!hdfs.isDir(clust, conf)) {
			System.out.println("clust is not a directory");
			launch = false;
		}

		if (!hdfs.exists(input + "/step-" + step, conf)) {
			System.out.println(input + "/step-" + step
					+ " directory does not exist");
			launch = false;
		} else if (!hdfs.isDir(input + "/step-" + step, conf)) {
			System.out.println(input + "/step-" + step + " is not a directory");
			launch = false;
		}

		if (!hdfs.exists(clust + "/step-" + step, conf)) {
			System.out.println(clust + "/step-" + step
					+ " directory does not exist");
			if (hdfs.makeDir(clust + "/step-" + step, conf)) {
				System.out.println(clust + "/step-" + step
						+ " directory created");

				System.out
						.println("please copy dict file to newly created folder");
				launch = false;

			} else {
				System.out.println("failed to create " + clust + "/step-"
						+ step + " directory");
				return false;
			}
		} else if (!hdfs.isDir(clust + "/step-" + step, conf)) {
			System.out.println(clust + "/step-" + step + " is not a directory");
			launch = false;
		}

		if (option == null) {
			conf.set("option", "p");
		} else if (!(option.equals("p") || option.equals("l"))) {
			System.out
					.println("incorrect option please provide: p - for parallel or l - for linear");
		}
		if (connType == null) {
			conf.set("connType", "CompleteLink");
		} else if (!connType.equals("CompleteLink")
				&& !connType.equals("SingleLink")) {
			System.out
					.println("incorrect connType, please provide CompleteLink or SingleLink");
		}

		return launch;
	}

	private void confPropertiesInit() {

		Configuration conf = this.getConf();
		conf.set("output.clusterfilenames", "clust");
		int step = conf.getInt("step", 0);

		conf.set("output", conf.get("input") + "/step-" + (step + 1));
		conf.set("input", conf.get("input") + "/step-" + step);
		conf.set("clust", conf.get("clust") + "/step-" + step);

	}

	private boolean runStep(int step) {
		try {
			Configuration conf = this.getConf();

			DistributedCache.addCacheFile(new URI(conf.get("dict")), conf);
			DistributedCache.addCacheFile(new URI(conf.get("clust")), conf);

			System.out.println("run " + step);
			System.out.println("input:  " + conf.get("input"));
			System.out.println("output: " + conf.get("output"));
			System.out.println("dict:   " + conf.get("dict"));
			System.out.println("clust:  " + conf.get("clust"));
			System.out.println("option: " + conf.get("option"));
			System.out.println("connType: " + conf.get("connType"));
			Job job = setupJob();
			// job.setNumReduceTasks(0);
			// t = System.nanoTime();
			boolean res = job.waitForCompletion(true);
			// jobtimes.add(System.nanoTime() - t);
			printCounters(job.getCounters());
			return res;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	private Job setupJob() throws IOException {

		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf);
		Path inputPath = new Path(conf.get("input"));
		Path outputDir = new Path(conf.get("output"));

		job.setJarByClass(HierClustDriver.class);

		job.setMapperClass(HierClustMapper.class);
		job.setReducerClass(HierClustReducer.class);
		job.setNumReduceTasks(1);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, outputDir);
		job.setOutputFormatClass(TextOutputFormat.class);

		MultipleOutputs.addNamedOutput(job,
				conf.get("output.clusterfilenames"), TextOutputFormat.class,
				Text.class, Text.class);

		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		return job;

	}

	private String moveClusterInfo(int step) {

		Configuration conf = this.getConf();
		String clustInfDir = conf.get("clust").substring(0,
				conf.get("clust").lastIndexOf("-"))
				+ "-" + (step + 1);
		this.hdfs.makeDir(clustInfDir, conf);

		if (this.hdfs.moveFile("hierclust2/input/step-" + (step + 1),
				clustInfDir + "/clust-r-00000", "clust", conf)) {
			return clustInfDir;
		} else
			return null;

	}

	private int confPropertiesIncrement() {

		Configuration conf = this.getConf();
		int step = conf.getInt("step", 0) + 1;
		conf.setInt("step", step);
		conf.set(
				"output",
				conf.get("output").substring(0,
						conf.get("output").lastIndexOf("-"))
						+ "-" + (step + 1));
		conf.set(
				"input",
				conf.get("input").substring(0,
						conf.get("input").lastIndexOf("-"))
						+ "-" + step);
		conf.set(
				"clust",
				conf.get("clust").substring(0,
						conf.get("clust").lastIndexOf("-"))
						+ "-" + step);
		return step;

	}

	private void printCounters(Counters counters) {
		Counter c1 = counters.findCounter(COMPLEXITY_COUNTER.IF_CNT);
		System.out.println(c1.getDisplayName() + ":" + c1.getValue());
		ifCnt += c1.getValue();
		c1 = counters.findCounter(COMPLEXITY_COUNTER.ASS_CNT);
		System.out.println(c1.getDisplayName() + ":" + c1.getValue());
		assCnt += c1.getValue();

	}
}
