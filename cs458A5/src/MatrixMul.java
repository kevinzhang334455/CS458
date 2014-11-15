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

public class MatrixMul {

  public static class MatrixMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, FloatWritable> {
    private IntWritable curr_row = new IntWritable();
    private FloatWritable row_result = new FloatWritable();
    private URI[] vec_file;
    private List<Float> vec = new ArrayList<Float>();

    public void configure(JobConf job) {
      try {
       String strLine;
       FileSystem fs = FileSystem.get(job);
       vec_file = DistributedCache.getCacheFiles(job);
       FSDataInputStream fis = fs.open(new Path(vec_file[0].getPath()));
       while ((strLine = fis.readLine()) != null) {
        vec.add(Float.parseFloat(strLine));
       }
       fis.close();
     } catch (IOException ioe) {
       System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
     }
   }

   public void map (LongWritable key, Text value, OutputCollector<IntWritable, FloatWritable> output, Reporter report) throws IOException {
     int counter = 0;
     int rowindex = 0;
     int colindex = 0;
     float elevalue = 0;
     float result = 0;
     float vec_value = 0;

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
        elevalue = Float.parseFloat(iter.nextToken());
        break;
      }
      counter = (counter+1)%3;
    }

    elevalue = elevalue * vec.get(colindex-1);
    curr_row.set(rowindex);
    row_result.set(elevalue);
    output.collect(curr_row, row_result);
  }
}

public static class SumReducer extends MapReduceBase implements Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {
  private FloatWritable row_result_sum = new FloatWritable();

  public void reduce (IntWritable key, Iterator<FloatWritable> values, OutputCollector<IntWritable, FloatWritable> output, Reporter report) throws IOException {
    float sum = 0;
    while (values.hasNext()) {
     sum += values.next().get();
   }
   row_result_sum.set(sum);
   output.collect(key, row_result_sum);
 }
}

public static void main (String[] args) throws Exception {
  Configuration conf = new Configuration();
  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
  if (otherArgs.length != 3) {
    System.err.println("Usage: MatrixMul <filein> <vec_path> <out>");
    System.exit(2);
  }

  JobConf job = new JobConf(MatrixMul.class);

  job.setMapOutputKeyClass(IntWritable.class);
  job.setMapOutputValueClass(FloatWritable.class);
  job.setOutputKeyClass(IntWritable.class);
  job.setOutputValueClass(FloatWritable.class); 

  DistributedCache.addCacheFile(new URI(otherArgs[1]), job);

  job.setInputFormat(TextInputFormat.class);
  job.setOutputFormat(TextOutputFormat.class);


  job.setMapperClass(MatrixMapper.class);
  job.setReducerClass(SumReducer.class);

  FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
  FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

  JobClient.runJob(job);
}
}
