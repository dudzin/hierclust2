package pl.pw.elka.test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import pl.pw.elka.hierclust.HierClustMapper;
import pl.pw.elka.hierclust.HierClustReducer;

public class HierClustTest {
	MapDriver<LongWritable, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> combinerDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {

		HierClustMapper mapper = new HierClustMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);

		HierClustReducer reducer = new HierClustReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);
		reduceDriver.getConfiguration().set("output.clusterfilenames", "clust");

	}

	@Test
	public void MapperParallelReadTest() throws IOException, URISyntaxException {

		System.out.println("MapperParallelReadTest()");

		String dictPath = "/dictionary1";
		String clustPath = "/clusters1";
		setMapDriverConf(dictPath, clustPath, "p");

		mapDriver.getConfiguration().set("input.clust", "step-0");
		Text in1 = new Text("0	0,2,4");
		Text in2 = new Text("1	2,0,5");
		Text in3 = new Text("2	4,5,0");

		Text key1 = new Text("0&1");
		Text key2 = new Text("0&1");
		Text key3 = new Text("0&2");

		Text val1 = new Text("0,2,4");
		Text val2 = new Text("2,0,5");
		Text val3 = new Text("4,5,0");

		mapDriver.withInput(new LongWritable(0), in1);
		mapDriver.withInput(new LongWritable(0), in2);
		mapDriver.withInput(new LongWritable(0), in3);

		mapDriver.withOutput(key1, val1);
		mapDriver.withOutput(key2, val2);
		mapDriver.withOutput(key3, val3);

		// mapDriver.runTest();
	}

	@Test
	public void MapperParallelReadTest2() throws IOException,
			URISyntaxException {

		System.out.println("MapperParallelReadTest2()");

		String dictPath = "/dictionary2";
		String clustPath = "/clusters2";
		setMapDriverConf(dictPath, clustPath, "p");

		mapDriver.getConfiguration().set("input.clust", "file1");
		Text in1 = new Text("0&1	0,0,2,3,2");
		Text in2 = new Text("2	2,3,0,2,5");
		Text in3 = new Text("3	3,3,2,0,4");
		Text in4 = new Text("4	4,2,3,4,0");

		Text key1 = new Text("0&1&2");
		Text key2 = new Text("0&1&2");
		Text key3 = new Text("2&3");
		Text key4 = new Text("0&1&4");

		Text val1 = new Text("0,0,2,3,2");
		Text val2 = new Text("2,2,0,2,5");
		Text val3 = new Text("3,3,2,0,4");
		Text val4 = new Text("2,2,3,4,0");

		mapDriver.withInput(new LongWritable(0), in1);
		mapDriver.withInput(new LongWritable(0), in2);
		mapDriver.withInput(new LongWritable(0), in3);
		mapDriver.withInput(new LongWritable(0), in4);

		mapDriver.withOutput(key1, val1);
		mapDriver.withOutput(key2, val2);
		mapDriver.withOutput(key3, val3);
		mapDriver.withOutput(key4, val4);

		// mapDriver.runTest();
	}

	@Test
	public void MapperParallelReadTest3() throws IOException,
			URISyntaxException {

		System.out.println("MapperParallelReadTest3()");

		String dictPath = "/dictionary2";
		String clustPath = "/clusters3";
		setMapDriverConf(dictPath, clustPath, "p");

		mapDriver.getConfiguration().set("input.clust", "file1");
		Text in1 = new Text("0&1&2	0,0,0,3,4");
		Text in2 = new Text("3	3,3,2,0,4");
		Text in3 = new Text("4	4,2,3,4,0");

		Text key1 = new Text("0&1&2&3");
		Text key2 = new Text("0&1&2&3");
		Text key3 = new Text("0&1&2&4");

		Text val1 = new Text("0,0,0,3,4");
		Text val2 = new Text("2,2,2,0,4");
		Text val3 = new Text("2,2,2,4,0");

		mapDriver.withInput(new LongWritable(0), in1);
		mapDriver.withInput(new LongWritable(0), in2);
		mapDriver.withInput(new LongWritable(0), in3);

		mapDriver.withOutput(key1, val1);
		mapDriver.withOutput(key2, val2);
		mapDriver.withOutput(key3, val3);

		// mapDriver.runTest();
	}

	@Test
	public void MapperParallelReadTest4() throws IOException,
			URISyntaxException {

		System.out.println("MapperParallelReadTest3()");

		String dictPath = "/dictionary3";
		String clustPath = "/clusters4";
		setMapDriverConf(dictPath, clustPath, "p");

		mapDriver.getConfiguration().set("input.clust", "file1");
		Text in1 = new Text("0	0,32,38,26,36,46,44");
		Text in2 = new Text("1	32,0,38,26,42,46,44");
		Text in3 = new Text("2&3	26,26,0,0,28,34,32");
		Text in4 = new Text("4	36,42,36,28,0,48,48");
		Text in5 = new Text("5&6	44,44,44,32,48,0,0");

		Text key1 = new Text("0&2&3");
		Text key2 = new Text("1&2&3");
		Text key3 = new Text("0&2&3");
		Text key4 = new Text("2&3&4");
		Text key5 = new Text("2&3&5&6");

		Text val1 = new Text("0,32,26,26,36,44,44");
		Text val2 = new Text("32,0,26,26,42,44,44");
		Text val3 = new Text("26,26,0,0,28,32,32");
		Text val4 = new Text("36,42,28,28,0,48,48");
		Text val5 = new Text("44,44,32,32,48,0,0");

		mapDriver.withInput(new LongWritable(0), in1);
		mapDriver.withInput(new LongWritable(0), in2);
		mapDriver.withInput(new LongWritable(0), in3);
		mapDriver.withInput(new LongWritable(0), in4);
		mapDriver.withInput(new LongWritable(0), in5);

		mapDriver.withOutput(key1, val1);
		mapDriver.withOutput(key2, val2);
		mapDriver.withOutput(key3, val3);
		mapDriver.withOutput(key4, val4);
		mapDriver.withOutput(key5, val5);

		// mapDriver.runTest();
	}

	@Test
	public void MapperParallelReadTest5() throws IOException,
			URISyntaxException {

		System.out.println("MapperParallelReadTes5t()");

		String dictPath = "/dictionary1";
		String clustPath = "/clusters1";
		setMapDriverConf(dictPath, clustPath, "p");

		mapDriver.getConfiguration().set("input.clust", "step-0");
		Text in1 = new Text("0	0,0,4");
		Text in2 = new Text("1	0,0,5");
		Text in3 = new Text("2	4,5,0");

		Text key1 = new Text("0,1");
		Text key2 = new Text("0,1");
		Text key3 = new Text("0,2");

		Text val1 = new Text("0,0,4");
		Text val2 = new Text("0,0,5");
		Text val3 = new Text("4,5,0");

		mapDriver.withInput(new LongWritable(0), in1);
		mapDriver.withInput(new LongWritable(0), in2);
		mapDriver.withInput(new LongWritable(0), in3);

		mapDriver.withOutput(key1, val1);
		mapDriver.withOutput(key2, val2);
		mapDriver.withOutput(key3, val3);

		// mapDriver.runTest();
	}

	@Test
	public void MapperLinearReadTest1() throws IOException, URISyntaxException {

		System.out.println("MapperLinearReadTest1()");

		String dictPath = "/dictionary2";
		String clustPath = "/clusters2";
		setMapDriverConf(dictPath, clustPath, "l");

		mapDriver.getConfiguration().set("input.clust", "file1");
		Text in1 = new Text("0&1	0,0,2,9,9");
		Text in2 = new Text("2	2,3,0,9,9");
		Text in3 = new Text("3	9,9,9,0,4");
		Text in4 = new Text("4	9,9,9,4,0");

		Text key1 = new Text("1");
		Text key2 = new Text("1");
		Text key3 = new Text("1");
		Text key4 = new Text("1");

		Text val1 = new Text("0&1&2@2@0,0,2,9,9");
		Text val2 = new Text("0&1&2@2@2,2,0,9,9");
		Text val3 = new Text("3&4@4@9,9,9,0,4");
		Text val4 = new Text("3&4@4@9,9,9,4,0");

		mapDriver.withInput(new LongWritable(0), in1);
		mapDriver.withInput(new LongWritable(0), in2);
		mapDriver.withInput(new LongWritable(0), in3);
		mapDriver.withInput(new LongWritable(0), in4);

		mapDriver.withOutput(key1, val1);
		mapDriver.withOutput(key2, val2);
		mapDriver.withOutput(key3, val3);
		mapDriver.withOutput(key4, val4);

		// mapDriver.runTest();
	}

	@Test
	public void MapperLinearReadTest2() throws IOException, URISyntaxException {

		System.out.println("MapperLinearReadTest2()");

		String dictPath = "/dictionary3";
		String clustPath = "/clusters4";
		setMapDriverConf(dictPath, clustPath, "l");

		mapDriver.getConfiguration().set("input.clust", "file1");
		Text in2 = new Text("0	0,32,38,26,36,46,44");
		Text in3 = new Text("1	32,0,38,26,42,46,44");
		Text in1 = new Text("2&3	26,26,0,0,28,34,32");
		Text in4 = new Text("4	36,42,36,28,0,48,48");
		Text in5 = new Text("5&6	44,44,44,32,48,0,0");

		Text key1 = new Text("1");
		Text key2 = new Text("1");
		Text key3 = new Text("1");
		Text key4 = new Text("1");
		Text key5 = new Text("1");

		Text val2 = new Text("0&2&3@26@0,32,26,26,36,44,44");
		Text val3 = new Text("1&2&3@26@32,0,26,26,42,44,44");
		Text val1 = new Text("0&2&3@26@26,26,0,0,28,32,32");
		Text val4 = new Text("2&3&4@28@36,42,28,28,0,48,48");
		Text val5 = new Text("2&3&5&6@32@44,44,32,32,48,0,0");

		mapDriver.withInput(new LongWritable(0), in1);
		mapDriver.withInput(new LongWritable(0), in2);
		mapDriver.withInput(new LongWritable(0), in3);
		mapDriver.withInput(new LongWritable(0), in4);
		mapDriver.withInput(new LongWritable(0), in5);

		mapDriver.withOutput(key1, val1);
		mapDriver.withOutput(key2, val2);
		mapDriver.withOutput(key3, val3);
		mapDriver.withOutput(key4, val4);
		mapDriver.withOutput(key5, val5);

		// mapDriver.runTest();
	}

	@Test
	public void ReducerParallelTest() throws IOException, URISyntaxException {

		System.out.println("ReducerParallelTest()");

		String dictPath = "/dictionary2";
		String clustPath = "/clusters2";
		setReduceDriverConf(dictPath, clustPath, "p");

		Text key1 = new Text("0&1");
		Text key2 = new Text("0&2");

		List<Text> inVal1 = new ArrayList<Text>();
		inVal1.add(new Text("0,2,4"));
		inVal1.add(new Text("2,0,5"));

		List<Text> inVal2 = new ArrayList<Text>();
		inVal2.add(new Text("4,5,0"));

		Text outkey1 = new Text("0&1");
		Text outkey2 = new Text("2");
		Text outval1 = new Text("0,0,4");
		Text outval2 = new Text("4,5,0");

		reduceDriver.withInput(key1, inVal1);
		reduceDriver.withInput(key2, inVal2);
		reduceDriver.withOutput(outkey1, outval1);
		reduceDriver.withOutput(outkey2, outval2);

		reduceDriver.runTest();

	}

	@Test
	public void ReducerParallelTest2() throws IOException, URISyntaxException {

		System.out.println("ReducerParallelTest2()");

		String dictPath = "/dictionary2";
		String clustPath = "/clusters2";
		setReduceDriverConf(dictPath, clustPath, "p");

		Text key1 = new Text("0&1&2");
		Text key2 = new Text("2&3");
		Text key3 = new Text("0&1&4");

		List<Text> inVal1 = new ArrayList<Text>();
		inVal1.add(new Text("0,0,2,3,2"));
		inVal1.add(new Text("2,3,0,2,3"));
		List<Text> inVal2 = new ArrayList<Text>();
		inVal2.add(new Text("3,3,2,0,4"));
		List<Text> inVal3 = new ArrayList<Text>();
		inVal3.add(new Text("4,2,3,4,0"));

		Text outkey1 = new Text("0&1&2");
		Text outkey2 = new Text("3");
		Text outkey3 = new Text("4");
		Text outval1 = new Text("0,0,0,2,2");
		Text outval2 = new Text("3,3,2,0,4");
		Text outval3 = new Text("4,2,3,4,0");

		reduceDriver.withInput(key1, inVal1);
		reduceDriver.withInput(key2, inVal2);
		reduceDriver.withInput(key3, inVal3);
		reduceDriver.withOutput(outkey1, outval1);
		reduceDriver.withOutput(outkey2, outval2);
		reduceDriver.withOutput(outkey3, outval3);

		reduceDriver.runTest();

	}

	@Test
	public void ReducerParallelTest3() throws IOException, URISyntaxException {

		System.out.println("ReducerParallelTest3()");

		String dictPath = "/dictionary2";
		String clustPath = "/clusters2";
		setReduceDriverConf(dictPath, clustPath, "p");

		Text key1 = new Text("0&1&2&3");
		Text key2 = new Text("0&1&2&3");
		Text key3 = new Text("0&1&2&4");

		List<Text> inVal1 = new ArrayList<Text>();
		inVal1.add(new Text("0,0,0,3,2"));
		inVal1.add(new Text("3,3,2,0,4"));
		List<Text> inVal2 = new ArrayList<Text>();
		inVal2.add(new Text("4,2,3,4,0"));

		Text outkey1 = new Text("0&1&2&3");
		Text outkey2 = new Text("4");
		Text outval1 = new Text("0,0,0,0,2");
		Text outval2 = new Text("4,2,3,4,0");

		reduceDriver.withInput(key1, inVal1);
		reduceDriver.withInput(key2, inVal2);
		reduceDriver.withOutput(outkey1, outval1);
		reduceDriver.withOutput(outkey2, outval2);

		reduceDriver.runTest();

	}

	@Test
	public void ReducerParallelTest4() throws IOException, URISyntaxException {

		System.out.println("ReducerParallelTest3()");

		String dictPath = "/dictionary3";
		String clustPath = "/clusters4";
		setReduceDriverConf(dictPath, clustPath, "p");

		Text key1 = new Text("0&2&3");
		Text key2 = new Text("1&2&3");
		Text key3 = new Text("2&3&4");
		Text key4 = new Text("2&3&5&6");

		List<Text> inVal1 = new ArrayList<Text>();
		inVal1.add(new Text("0,32,38,26,36,46,44"));
		inVal1.add(new Text("26,26,0,0,28,34,32"));
		List<Text> inVal2 = new ArrayList<Text>();
		inVal2.add(new Text("32,0,38,26,42,46,44"));
		List<Text> inVal3 = new ArrayList<Text>();
		inVal3.add(new Text("36,42,36,28,0,48,48"));
		List<Text> inVal4 = new ArrayList<Text>();
		inVal4.add(new Text("44,44,44,32,48,0,0"));

		Text outkey1 = new Text("0&2&3");
		Text outkey2 = new Text("1");
		Text outkey3 = new Text("4");
		Text outkey4 = new Text("5&6");

		Text outval1 = new Text("0,26,0,0,28,34,32");
		Text outval2 = new Text("32,0,38,26,42,46,44");
		Text outval3 = new Text("36,42,36,28,0,48,48");
		Text outval4 = new Text("44,44,44,32,48,0,0");

		reduceDriver.withInput(key1, inVal1);
		reduceDriver.withInput(key2, inVal2);
		reduceDriver.withInput(key3, inVal3);
		reduceDriver.withInput(key4, inVal4);
		reduceDriver.withOutput(outkey1, outval1);
		reduceDriver.withOutput(outkey2, outval2);
		reduceDriver.withOutput(outkey3, outval3);
		reduceDriver.withOutput(outkey4, outval4);

		reduceDriver.runTest();

	}

	@Test
	public void ReducerLinearTest() throws IOException, URISyntaxException {

		System.out.println("ReducerLinearTest()");

		String dictPath = "/dictionary2";
		String clustPath = "/clusters2";
		setReduceDriverConf(dictPath, clustPath, "l");

		Text key1 = new Text("1");

		List<Text> inVal1 = new ArrayList<Text>();
		inVal1.add(new Text("0&1&2@2@0,0,2,9,9"));
		inVal1.add(new Text("0&1&2@2@2,3,0,9,9"));
		inVal1.add(new Text("3&4@4@9,9,9,0,4"));
		inVal1.add(new Text("3&4@4@9,9,9,4,0"));

		Text outkey1 = new Text("0&1&2");
		Text outkey2 = new Text("3");
		Text outkey3 = new Text("4");
		Text outval1 = new Text("0,0,0,9,9");
		Text outval2 = new Text("9,9,9,0,4");
		Text outval3 = new Text("9,9,9,4,0");

		reduceDriver.withInput(key1, inVal1);
		reduceDriver.withOutput(outkey1, outval1);
		reduceDriver.withOutput(outkey2, outval2);
		reduceDriver.withOutput(outkey3, outval3);

		reduceDriver.runTest();

	}

	@Test
	public void ReducerLinearTest2() throws IOException, URISyntaxException {

		System.out.println("ReducerLinearTest2()");

		String dictPath = "/dictionary3";
		String clustPath = "/clusters4";
		setReduceDriverConf(dictPath, clustPath, "l");

		Text key1 = new Text("1");

		List<Text> inVal1 = new ArrayList<Text>();
		inVal1.add(new Text("0&2&3@26@26,26,0,0,28,34,32"));
		inVal1.add(new Text("1&2&3@26@32,0,38,26,42,46,44"));
		inVal1.add(new Text("0&2&3@26@0,32,38,26,36,46,44"));
		inVal1.add(new Text("2&3&4@28@36,42,36,28,0,48,48"));
		inVal1.add(new Text("2&3&5,6@32@44,44,44,32,48,0,0"));

		Text outkey1 = new Text("0&2&3");
		Text outkey2 = new Text("1");
		Text outkey3 = new Text("4");
		Text outkey4 = new Text("5&6");

		Text outval1 = new Text("0,26,0,0,28,34,32");
		Text outval2 = new Text("32,0,38,26,42,46,44");
		Text outval3 = new Text("36,42,36,28,0,48,48");
		Text outval4 = new Text("44,44,44,32,48,0,0");

		reduceDriver.withInput(key1, inVal1);
		reduceDriver.withOutput(outkey1, outval1);
		reduceDriver.withOutput(outkey2, outval2);
		reduceDriver.withOutput(outkey3, outval3);
		reduceDriver.withOutput(outkey4, outval4);

		reduceDriver.runTest();

	}

	@Test
	public void TTest() {
		System.out.println("TTest()");

		HierClustMapper hm = new HierClustMapper();
		hm.setConnType("SingleLink");
		ArrayList<String> dict = new ArrayList<String>();
		dict.add("file1");
		dict.add("file2");
		dict.add("file3");
		dict.add("file4");
		dict.add("file5");
		dict.add("file6");

		hm.setDict(dict);
		System.out.println("pos " + "0&1");
		ArrayList<String> clust = new ArrayList<String>();
		clust.add("0&2&3	5");
		clust.add("1	0");
		clust.add("4	0");
		clust.add("5	0");
		String row = "4,0,3,6,7,8";
		// hm.setClust(clust);
		// String newrow = hm.recalcRow(row);

		// assertEquals(newrow, "3,0,3,3,7,8");

	}

	private void setMapDriverConf(String dictPath, String clustPath,
			String option) throws URISyntaxException {

		mapDriver.getConfiguration().set("dict", dictPath);
		mapDriver.getConfiguration().set("clust", clustPath);
		mapDriver.getConfiguration().set("option", option);
		mapDriver.addCacheFile(clustPath);
		mapDriver.addCacheFile(dictPath);
		mapDriver.getConfiguration().set("connType", "SingleLink");

	}

	private void setReduceDriverConf(String dictPath, String clustPath,
			String option) throws URISyntaxException {

		DistributedCache.addCacheFile(new URI(dictPath),
				mapDriver.getConfiguration());
		reduceDriver.getConfiguration().set("dict", dictPath);
		DistributedCache.addCacheFile(new URI(clustPath),
				mapDriver.getConfiguration());
		reduceDriver.getConfiguration().set("clust", clustPath);
		reduceDriver.getConfiguration().set("option", option);
		reduceDriver.getConfiguration().set("connType", "SingleLink");
	}
}
