import java.io.*;
import java.util.*;
import java.util.regex.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordCount {

  public static class TokenizerMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();
    private Text filename = new Text();
    private String inputFile = new String();
   
    public void map (LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter report) throws IOException {
      FileSplit filesplit = (FileSplit) report.getInputSplit();
      inputFile = filesplit.getPath().getName();
      Pattern p = Pattern.compile("[\\pP\\pM\\pS\\pN\\pC]"); //parsing text to white space expect words : idea from binyu hu
      Matcher m = p.matcher(value.toString().toLowerCase());
      String line = m.replaceAll(" ");

      StringTokenizer iter = new StringTokenizer(line);
      while (iter.hasMoreTokens()) {
	      word.set(iter.nextToken());
	      filename.set(inputFile);
        output.collect(word, filename);
      }
    }
  }


  public static class IntSumReducer extends MapReduceBase implements Reducer<Text, Text, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce (Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter report) throws IOException {
      int sum = 0;
      String lastfilename = new String();
      while (values.hasNext()) {
	String currfilename = values.next().toString();
	if (!currfilename.equals(lastfilename)) {
	  lastfilename = currfilename;
	  sum = sum + 1;
	}
      }
      result.set(sum);
      output.collect(key,result);
    }
  }

  public static void main (String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }

    JobConf job = new JobConf(WordCount.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setInputFormat(TextInputFormat.class);
    job.setOutputFormat(TextOutputFormat.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    JobClient.runJob(job);
  }
}
