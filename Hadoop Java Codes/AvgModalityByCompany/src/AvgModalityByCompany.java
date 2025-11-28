import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgModalityByCompany {
	public static enum MY_COUNTER { RECORDS_PROCESSED }

    public static class ModMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text company = new Text();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.toLowerCase().contains("tool_name")) return; // skip header
            String[] fields = line.split(",", -1);
            if (fields.length > 21) {
                company.set(fields[1].trim()); // company column
                try {
                	int modalityCount = Integer.parseInt(fields[21].trim());
                    context.write(company, new IntWritable(modalityCount));
                } catch (NumberFormatException e) {}
            }
            context.getCounter(MY_COUNTER.RECORDS_PROCESSED).increment(1);
        }
    }
    
    public static class ModReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        private DoubleWritable out = new DoubleWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (IntWritable v : values) {
                sum += v.get();
                count++;
            }
            if (count > 0) {
                out.set(sum*1.0/count);
                context.write(key, out);
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        long start = System.nanoTime();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Avg Modality By Company");
        job.setJarByClass(AvgModalityByCompany.class);
        
        job.setMapperClass(ModMapper.class);
        job.setReducerClass(ModReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean ok = job.waitForCompletion(true);

        long end = System.nanoTime();
        double seconds = (end - start)/1e9;
        long records = job.getCounters().findCounter(MY_COUNTER.RECORDS_PROCESSED).getValue();
        double throughput = (seconds>0)?(records/seconds):records;
        
        System.out.println("Execution Time (s): " + seconds);
        System.out.println("Records processed: " + records);
        System.out.println("Throughput (records/s): " + throughput);
        
        Runtime rt = Runtime.getRuntime();
        System.out.println("Used Memory (MB): " + (rt.totalMemory()-rt.freeMemory())/(1024.0*1024.0));
        MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
        System.out.println("Heap Used (MB): " + mem.getHeapMemoryUsage().getUsed()/(1024.0*1024.0));
        
        System.exit(ok ? 0 : 1);
    }
}