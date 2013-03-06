import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;

public class MapReduceApriory {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		private Text word = new Text();
		public static LinkedHashMap<Long, List<String>> transMap = new LinkedHashMap<Long, List<String>>();
		private final static IntWritable one = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter report)
				throws IOException {
			String line = value.toString();
			System.out.println("Inside Map Now");
			StringTokenizer st = new StringTokenizer(line, "\n");

			List<String> totalItemsList = new ArrayList<String>();
			long transCount = 0;			
			
			Configuration config = HBaseConfiguration.create();
			HTable table = new HTable(config, "Career");
			Put p = new Put(Bytes.toBytes("Key"));

			while (st.hasMoreTokens()) {
				transCount += 1;
				String transValues = st.nextToken();

				p.add(Bytes.toBytes("input"),
						Bytes.toBytes(String.valueOf(transCount)),
						Bytes.toBytes(transValues));

				StringTokenizer st1 = new StringTokenizer(transValues, ";");
				List<String> transValuesList = new ArrayList<String>();
				while (st1.hasMoreTokens()) {
					String transValue = st1.nextToken();
					transValuesList.add(transValue);
					if (!totalItemsList.contains(transValue)) {
						totalItemsList.add(transValue);
					}
				}
				transMap.put(transCount, transValuesList);
			}

			if (!p.isEmpty()) {
				System.out.println("The count of Put is: " + p.size());
				table.put(p);
				table.close();
			} else {
				System.out.println("Put is empty in Map");
			}

			System.out.println("The Size of the Transaction Map is: "
					+ transMap.size());
			System.out.println("The Total Number of Items are: "
					+ totalItemsList.size());

			for (Long transaction : transMap.keySet()) {
				List<String> values = transMap.get(transaction);
				for (int count = 0; count < totalItemsList.size(); count++) {
					if (values.contains(totalItemsList.get(count))) {
						word.set(totalItemsList.get(count));
						output.collect(word, one);
					}
				}
			}

			word.set("done" + "#" + 2);

			output.collect(word, one);
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, LongWritable, Text> {

		private static final int TOTAL_RECORD_COUNT = 51;
		private static final double SUPPORT = 0.5;
		public static List<String> intermediateValues = new LinkedList<String>();

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {

			int count = 0;

			while (values.hasNext()) {
				count += values.next().get();
			}

			String[] keycounts = key.toString().split("#");
			String keyString = keycounts[0];
			String countString = null;

			if (keycounts.length > 1) {
				countString = keycounts[1];
			}
			Configuration config = HBaseConfiguration.create();
			HTable table = new HTable(config, "Career");
			Put p = new Put(Bytes.toBytes("Output"));

			if (count >= ((SUPPORT * TOTAL_RECORD_COUNT) / 100)) {

				p.add(Bytes.toBytes("count"), Bytes.toBytes(keyString),
						Bytes.toBytes(String.valueOf(count)));
				intermediateValues.add(keyString);
			}

			if (!p.isEmpty()) {
				table.put(p);
				table.close();
			}

			if (countString != null) {
				if (intermediateValues.size() > 0) {
					Collections.sort(intermediateValues, new Comparator() {
						public int compare(Object o1, Object o2) {
							return ((Comparable<String>) (String) o1)
									.compareTo((String) o2);
						}
					});
					StringBuilder sb = new StringBuilder();
					for (String intermediateValue : intermediateValues) {
						sb.append("{" + intermediateValue + "};");
					}
					String collectOutput = sb.toString();
					collectOutput = collectOutput.substring(0,
							collectOutput.length() - 1);

					output.collect(new LongWritable(Long.valueOf(1)), new Text(
							collectOutput));

				}

			}

		}

	}

	public static class Map1 extends MapReduceBase implements
			Mapper<LongWritable, Text, LongWritable, Text> {
		
		private static final double SUPPORT = 0.5;
		private static final int TOTAL_RECORD_COUNT = 51;

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, Text> output, Reporter report)
				throws IOException {

			long i = key.get();
			i += 1;
			String line = value.toString();
			String nextLevelValues = "";
			StringTokenizer st = new StringTokenizer(line, ";");
			System.out.println("Inside Map1 Now");

			if (st.hasMoreTokens()) {
				List<String> tokens = new LinkedList<String>();
				while (st.hasMoreTokens()) {
					tokens.add(st.nextToken());
				}
				System.out.println("The total count of tokens is: "
						+ tokens.size());

				HashMap<String, Long> aprioryValues = aprioryGen(tokens, i,
						report);

				Configuration config = HBaseConfiguration.create();
				HTable table = new HTable(config, "Career");
				Put p = new Put(Bytes.toBytes("Output"));

				for (Entry<String, Long> aprioryEntry : aprioryValues
						.entrySet()) {

					if (aprioryEntry.getValue() >= ((SUPPORT * TOTAL_RECORD_COUNT) / 100)) {

						p.add(Bytes.toBytes("count"), Bytes
								.toBytes(aprioryEntry.getKey()),
								Bytes.toBytes(String.valueOf(aprioryEntry
										.getValue())));

						nextLevelValues += aprioryEntry.getKey() + ";";
					}

				}
				if (!p.isEmpty()) {
					table.put(p);
					table.close();
				}

				if (nextLevelValues != "") {
					nextLevelValues = nextLevelValues.substring(0,
							nextLevelValues.length() - 1);
					output.collect(new LongWritable(Long.valueOf(i)), new Text(
							nextLevelValues));

				}

			}

			if (nextLevelValues != "") {
				map(new LongWritable(i), new Text(nextLevelValues), output,
						report);
			}

		}

		private HashMap<String, Long> aprioryGen(List<String> tokens,
				long iteration, Reporter report) {
			HashMap<String, Long> finalMap = new LinkedHashMap<String, Long>();
			List<String> fullList = new LinkedList<String>();
			System.out.println("Inside Apriori Gen Now");
			for (int i = 0; i < (tokens.size() - 1); i++) {
				String stripi = tokens.get(i);
				if (stripi != null && stripi.length() >= 3) {
					stripi = stripi.substring(1, stripi.length() - 1);
				}

				if (iteration == 2) {
					// System.out.println("Inside Apriori Interation2 Now");
					for (int j = i + 1; j < tokens.size(); j++) {
						String stripj = tokens.get(j);
						if (stripj != null && stripj.length() >= 3) {
							stripj = stripj.substring(1, stripj.length() - 1);

							String value = "{" + stripi + ";" + stripj + "}";
							fullList.add(value);
						}
					}
				} else {
					// Iteration >= 3, Join Step
					System.out.println("Inside Iteration 3");
					String f1 = stripi;
					String l1 = stripi;
					System.out.println("StripI is: " + stripi);
					f1 = f1.substring(0, f1.lastIndexOf(";") + 1);
					l1 = l1.substring(l1.lastIndexOf(";") + 1);
					System.out.println("f1 : " + f1 + " l1: " + l1);
					for (int j = i + 1; j < tokens.size(); j++) {
						String stripj = tokens.get(j);
						stripj = stripj.substring(1, stripj.length() - 1);
						String f2 = stripj;
						String l2 = stripj;
						System.out.println("StripJ is: " + stripj);
						f2 = f2.substring(0, f2.lastIndexOf(";") + 1);
						l2 = l2.substring(l2.lastIndexOf(";") + 1);
						System.out.println("f2 : " + f2 + " l2: " + l2);
						if (f1.equalsIgnoreCase(f2)) {
							// Prune step
							System.out.println("Inside Prune Step");
							
							String prune = l1 + ";" + l2;
							boolean exists = false;
							for (int m = 0; m < tokens.size(); m++) {
								String stripm = tokens.get(m);
								if (stripm.indexOf(prune) != -1) {
									System.out.println("Match Exists!");
									exists = true;
								}
							}
							if (exists) {
								String value = "{" + f1 + prune + "}";
								System.out.println("New List Value is: "
										+ value);
								fullList.add(value);
							}

						}
					}
				}
			}
			
			HTable table = null;
			
			try {
				System.out
						.println("Inside Apriory Gen Comparation Try Catch Block");		
				Configuration config = HBaseConfiguration.create();
				table = new HTable(config, "Career");
				Get g = new Get(Bytes.toBytes("Key"));
				g.addFamily(Bytes.toBytes("input"));
				Result r = table.get(g);
				NavigableMap<byte[], byte[]> map = r.getFamilyMap(Bytes
						.toBytes("input"));

				System.out.println("The size of the map is: " + map.size());

				for (String listvalue : fullList) {

					System.out.println("The listvalue is: " + listvalue);
					String striplistvalue = listvalue.substring(1,
							listvalue.length() - 1);
					System.out.println("The value of striplistvalue is: "
							+ striplistvalue);

					long valuecounter = 0;

					for (byte[] value : map.values()) {
						report.setStatus("I am alive !");

						report.progress();
						
						StringTokenizer st = new StringTokenizer(striplistvalue,";");
						int counttokens = st.countTokens();
						String stringvalue = Bytes.toString(value);
						StringTokenizer st1 = new StringTokenizer(stringvalue,";");

						int tokenCounter = 0;

						while (st.hasMoreTokens()) {

							String token = st.nextToken();

							while (st1.hasMoreTokens()) {
								String dbvalue = st1.nextToken();

								if (token != null && dbvalue != null
										&& token.equalsIgnoreCase(dbvalue)) {
									tokenCounter++;
									st1 = new StringTokenizer(stringvalue,";");
									break;
								}
							}

						}

						if (tokenCounter == counttokens) {
							valuecounter++;
						}
					}
					
					if (valuecounter > 0) {
						finalMap.put(listvalue, valuecounter);
					}

				}

			} catch (IOException ioe) {
				ioe.printStackTrace();
			} finally
			{
				try {
					table.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			return finalMap;
		}

	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf();
		conf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, 2463);
		conf.setJobName("apriori");
		conf.setInputFormat(FixedLengthInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setJarByClass(MapReduceApriory.class);

		System.out.println("Started Job");

		JobConf mapConf = new JobConf();

		JobConf reduceConf = new JobConf();

		JobConf map1Conf = new JobConf();
		System.out.println("Calling addMapper 1");
		ChainMapper.addMapper(conf, Map.class, LongWritable.class, Text.class,
				Text.class, IntWritable.class, true, mapConf);
		System.out.println("Calling setReducer");

		ChainReducer.setReducer(conf, Reduce.class, Text.class,
				IntWritable.class, LongWritable.class, Text.class, true,
				reduceConf);
		System.out.println("Calling addMapper 2 now");
		ChainReducer.addMapper(conf, Map1.class, LongWritable.class,
				Text.class, LongWritable.class, Text.class, true, map1Conf);

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		/* System.exit(job.waitForCompletion(true) ? 0 : 1); */

		// JobClient jc = new JobClient(conf);
		System.out.println("RunJob Calling Now");
		JobClient.runJob(conf);
		// RunningJob rj = jc.submitJob(conf);
		// rj.waitForCompletion();

	}

}
