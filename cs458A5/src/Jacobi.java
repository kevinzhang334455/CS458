import java.io.*;
import java.util.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;

public class Jacobi {
	static enum MyCounters {Counter}

	public static class JacobiMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
		private IntWritable curr_row = new IntWritable();
	    private DoubleWritable row_result = new DoubleWritable();
		private URI[] xfiles;
		private double[] Xval;
		private int n;
		private int curr_iter;
		

		public void configure(JobConf job) {
			try {
				curr_iter = job.getInt("iteration", 1);
				n = job.getInt("n", 0);
				Xval = new double[n];
				Arrays.fill(Xval, 0.0);
				System.out.println("curr_iter:              " + curr_iter);
				System.out.println("n:              " + n);
				if (curr_iter > 1) {
					FileSystem fs = FileSystem.get(job);
					xfiles = DistributedCache.getCacheFiles(job);
					FSDataInputStream fis = fs.open(new Path(xfiles[1].getPath()));

					String strLine = fis.readLine();
					while (strLine != null) {
						String[] tokens = strLine.trim().split("\\s+");
						if (tokens.length != 2){
							throw new IOException("InputFile has improper format");
						}
						int col = Integer.parseInt(tokens[0]);
						double val = Double.parseDouble(tokens[1]);
						Xval[col] = val;
						// System.out.println("Xval: " + Xval.get(col));
						strLine = fis.readLine();
					}
					fis.close();
				}
			} catch (IOException ioe) {
				System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
			}
		}

		public void map (LongWritable key, Text value, OutputCollector<IntWritable, DoubleWritable> output, Reporter report) throws IOException {

			int counter = 0;
			int rowindex = 0;
			int colindex = 0;
			double elevalue = 0;
			double result = 0;
			double vec_value = 0;
            StringTokenizer iter = new StringTokenizer(value.toString());
            while (iter.hasMoreTokens()) {
            	switch (counter) {
            		case 0:
            		rowindex = Integer.parseInt(iter.nextToken());
            		break;
            		case 1:
            		colindex = Integer.parseInt(iter.nextToken());
            		break;
            		case 2:
            		elevalue = Double.parseDouble(iter.nextToken());
            		break;
            	}
            	counter = (counter+1)%3;
            }
            if (colindex != n) {
            	elevalue = elevalue * Xval[colindex];
            	System.out.println("rowindex" + rowindex + ":" + elevalue);
            	row_result.set(elevalue);
            	curr_row.set(rowindex);
            	output.collect(curr_row ,row_result);
            }
		}
	}

	public static class JacobiReducer extends MapReduceBase implements Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
		DoubleWritable sum_result = new DoubleWritable();
		private IntWritable curr_row = new IntWritable();

		public static double[] Aii;
		public static double[] Ain;
		public static double[] Xval;
		public static int n;
		public static int curr_iter;
		public static double eps;
		private URI[] xfiles;
		private int counter;

		public void configure(JobConf job) {
			try {
				curr_iter = job.getInt("iteration", 1);
				n = job.getInt("n", 0);
				eps = job.getFloat("eps", 0);
				System.out.println("curr_iter:              " + curr_iter);
				System.out.println("n:              " + n);
				Aii = new double[n];
				Ain = new double[n];
				Xval = new double[n];
				Arrays.fill(Aii, 0.0);
				Arrays.fill(Ain, 0.0);
				Arrays.fill(Xval, 0.0);
				FileSystem fs = FileSystem.get(job);
				xfiles = DistributedCache.getCacheFiles(job);
				FSDataInputStream fis = fs.open(new Path(xfiles[0].getPath()));	

				String strLine = fis.readLine();
				while (strLine != null) {
					String[] tokens = strLine.trim().split("\\s+");
					if (tokens.length != 3) {
						throw new IOException("InputFile has improper format");
					}
					int row = Integer.parseInt(tokens[0]);
					int col = Integer.parseInt(tokens[1]);
					double val = Double.parseDouble(tokens[2]);
					if (col == n)
						Ain[row] = val;
					else if (col == row)
						Aii[col] = val;
					strLine = fis.readLine();
					}
				fis.close();

				if (curr_iter > 1) {
					FSDataInputStream fis1 = fs.open(new Path(xfiles[1].getPath()));	
					String strLine1 = fis1.readLine();
					while (strLine1 != null) {
						String[] tokens1 = strLine1.trim().split("\\s+");
						if (tokens1.length != 2) {
							throw new IOException("InputFile has improper format");
						}
						int col1 = Integer.parseInt(tokens1[0]);
						double val1 = Double.parseDouble(tokens1[1]);
						Xval[col1] = val1;
						strLine1 = fis1.readLine();
					}

				fis1.close();

				}
				for (int i = 0; i<n; i++) {
					System.out.println("Ain: " + Ain[i] + "Aii: " + Aii[i]);
				}

			} catch (IOException ioe) {
				System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
			}
		}


		public void reduce (IntWritable key, Iterator<DoubleWritable> values, OutputCollector<IntWritable, DoubleWritable> output, Reporter report) throws IOException {
			double sum = 0.0;
			double elevalue = 0;
			while (values.hasNext()) {
				sum -= values.next().get();
			}
			System.out.println("sum" + key.get() + ": " + sum);
			// System.out.println("Ain: " + Ain[key.get()] + "Aii: " + Aii[key.get()]);
			System.out.println("Xval: " + Xval[key.get()]);
			sum += Ain[key.get()];
			sum /= Aii[key.get()];
			System.out.println("sum" + key.get() + ": " + sum);
			System.out.println("eps:" + eps);

			sum_result.set(sum);
			output.collect(key, sum_result);            
			if (Math.abs(sum - Xval[key.get()]) > eps)
				report.incrCounter(MyCounters.Counter, 1);
			System.out.println(report.getCounter(MyCounters.Counter));
		}
	}
	

	public static void main (String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: Jacobi <matrixAb> <max_iter> <significantDig>");
			System.exit(2);
		}
        int prefix = args[0].lastIndexOf('/') + 1;
        int suffix = args[0].lastIndexOf('.');
        int n = Integer.parseInt(args[0].substring(prefix, suffix));		
    	int max_iter = Integer.parseInt(args[1]);
    	float eps = 1;
    	for (int i = 0; i < Integer.parseInt(args[2]); i++)
    		eps = eps / 10;
    	int i = 1;
		long curr_num_not_meet = 0;
		while (i < max_iter) {
			Configuration conf = new Configuration();
			conf.setInt("n", n);
			conf.setFloat("eps", eps);
			conf.setInt("iteration", i);
			JobConf job = new JobConf(conf, Jacobi.class);
			FileSystem fs = FileSystem.get(job);
			String name = new String("Iteration" + i);
			job.setJobName(name);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(DoubleWritable.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(DoubleWritable.class);
			job.setInputFormat(TextInputFormat.class);
			job.setOutputFormat(TextOutputFormat.class);
			job.setMapperClass(JacobiMapper.class);
			job.setReducerClass(JacobiReducer.class);
			job.setNumMapTasks(2);
			DistributedCache.addCacheFile(new URI(args[0]), job);
			if ( i > 1) {
				String in = new String("/user/yzh145/Jacobi/depth_" + (i - 1) + "/" + "part-00000");
				DistributedCache.addCacheFile(new URI(in), job);								
			}
			Path out = new Path("/user/yzh145/Jacobi/depth_" + i);
			if (fs.exists(out))
				fs.delete(out, true);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, out);
			RunningJob parentjob = JobClient.runJob(job);
			curr_num_not_meet = parentjob.getCounters().getCounter(MyCounters.Counter); //idea of getting counter in main function is from xi jin 
			System.out.println("curr_num_not_meet: " + curr_num_not_meet);
			if (curr_num_not_meet == 0) break;
			else i = i + 1; 
		}
	}
}
