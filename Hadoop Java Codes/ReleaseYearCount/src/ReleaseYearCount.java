import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReleaseYearCount {

    public static enum MY_COUNTER { RECORDS_PROCESSED }

    public static enum MEM_COUNTER { HEAP_MB, NONHEAP_MB, JVM_COUNT }

    public static class YearMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text year = new Text();
        private boolean memoryRecorded = false;
        
        private void recordMemory(Context context) {
        	if (!memoryRecorded) {
                Runtime rt = Runtime.getRuntime();
                long heapMB = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
                MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
                long nonHeapMB = mem.getNonHeapMemoryUsage().getUsed() / (1024 * 1024);

                context.getCounter(MEM_COUNTER.HEAP_MB).increment(heapMB);
                context.getCounter(MEM_COUNTER.NONHEAP_MB).increment(nonHeapMB);
                context.getCounter(MEM_COUNTER.JVM_COUNT).increment(1);
                
                memoryRecorded = true;
        	}
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.toLowerCase().contains("tool_name")) return;
            
            String[] fields = line.split(",", -1);
            if (fields.length > 9) {
                year.set(fields[9].trim());
                context.write(year, one);
            }

            context.getCounter(MY_COUNTER.RECORDS_PROCESSED).increment(1);

            recordMemory(context);
        }
    }
    
    public static class YearReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable out = new IntWritable();
        private boolean memoryRecorded = false;
        
        private void recordMemory(Context context) {
        	if (!memoryRecorded) {
                Runtime rt = Runtime.getRuntime();
                long heapMB = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
                MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
                long nonHeapMB = mem.getNonHeapMemoryUsage().getUsed() / (1024 * 1024);


                context.getCounter(MEM_COUNTER.HEAP_MB).increment(heapMB);
                context.getCounter(MEM_COUNTER.NONHEAP_MB).increment(nonHeapMB);
                context.getCounter(MEM_COUNTER.JVM_COUNT).increment(1);

                memoryRecorded = true;
        	}
        }
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) sum += v.get();
            out.set(sum);
            context.write(key, out);
            
            recordMemory(context);
        }
    }
    
    
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ReleaseYearCount <input> <output>");
            System.exit(1);
        }

        long start = System.nanoTime();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Release Year Count");
        job.setJarByClass(ReleaseYearCount.class);

        job.setMapperClass(YearMapper.class);
        job.setCombinerClass(YearReducer.class);
        job.setReducerClass(YearReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        boolean ok = job.waitForCompletion(true);

        long end = System.nanoTime();
        double seconds = (end - start) / 1e9;
        
        long records = job.getCounters().findCounter(MY_COUNTER.RECORDS_PROCESSED).getValue();

        double throughput = (seconds > 0) ? (records / seconds) : records;
        
        long totalHeapMB = job.getCounters().findCounter(MEM_COUNTER.HEAP_MB).getValue();
        long totalNonHeapMB = job.getCounters().findCounter(MEM_COUNTER.NONHEAP_MB).getValue();
        long jvmCount = job.getCounters().findCounter(MEM_COUNTER.JVM_COUNT).getValue();
        
        double avgHeap = (jvmCount > 0) ? ((double) totalHeapMB / jvmCount) : 0;
        double avgNonHeap = (jvmCount > 0) ? ((double) totalNonHeapMB / jvmCount) : 0;
        
        System.out.println("===== Performance Metrics =====");
        System.out.println("Execution Time (s): " + seconds);
        System.out.println("Records Processed: " + records);
        System.out.println("Throughput (records/s): " + throughput);
        System.out.println("Heap Memory Used (MB): " + avgHeap);
        System.out.println("Non-Heap Memory Used (MB): " + avgNonHeap);

        
        System.exit(ok ? 0 : 1);
    }
}



    