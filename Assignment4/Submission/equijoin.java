import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The Class equijoin.
 */
public class equijoin {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {
		Path inputFile = new Path(args[0]);
		Path outputFile = new Path(args[1]);
		JobConf job = new JobConf(equijoin.class);
		job.setMapperClass(CreateMapper.class);
		job.setReducerClass(CreateReducer.class);
		job.setJobName("equijoin");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.set("mapred.textoutputformat.separator", " ");
		FileInputFormat.setInputPaths(job, inputFile);
		FileOutputFormat.setOutputPath(job, outputFile);
		try {
			JobClient.runJob(job);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * The Class CreateMapper.
	 */
	public static class CreateMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
		/** The join. */
		private Text join = new Text();
		
		/** The rows. */
		private Text rows = new Text();

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
		 */
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String oneLine[] = value.toString().split(",");
			int leng = oneLine.length;

			String relation = oneLine[0];
			String tup = relation;
			String keyjoins = oneLine[1];
			int m = 0;
			while (m < leng) {
				tup = tup + "," + oneLine[m];
				m += 1;
			}
			join.set(keyjoins);
			rows.set(tup);

			output.collect(join, rows);
		}
	}

	/**
	 * The Class CreateReducer.
	 */
	public static class CreateReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			List<String> table_first = new ArrayList<String>();
			List<String> table_second = new ArrayList<String>();
			List<String> object = new ArrayList<String>();

			Text outputValues = new Text();
			getList(values, table_first, table_second, object, outputValues);
			object = Lists.reverse(object);

			if (table_first.size() == 0 || table_second.size() == 0) {
				key.clear();
			} else {
				int m = 0;
				while (m < object.size()) {
					int n = m + 1;
					while (n < object.size()) {
						if (!object.get(m).split(",")[0].equalsIgnoreCase(object.get(n).split(", ")[0])) {
							outputValues.set(object.get(m) + " ," + object.get(n));
							output.collect(new Text(""), outputValues);
						}
						n += 1;
					}
					m += 1;
				}
			}
		}
	}

	/**
	 * Gets the list.
	 *
	 * @param values the values
	 * @param table_first the table first
	 * @param table_second the table second
	 * @param object the object
	 * @param outputValues the output values
	 * @return the list
	 */
	public static void getList(Iterator<Text> values, List<String> table_first, List<String> table_second,
			List<String> object, Text outputValues) {

		String table1 = null;
		boolean val = true;

		while (values.hasNext()) {
			String textValues = values.next().toString();
			String stringSplit[] = textValues.split(",");
			if (val == true) {
				table1 = stringSplit[0];
				val = false;
			}
			if (table1 == stringSplit[0]) {
				table_first.add(textValues);
			} else {
				table_second.add(textValues);
			}
			object.add(textValues);
		}
	}
}