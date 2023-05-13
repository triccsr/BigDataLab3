package nation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Nation {

    public static class Map extends Mapper<Object, Text, Text, DoubleWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] info=value.toString().split("\\|");
            Text nation = new Text(info[3]);
            DoubleWritable balance = new DoubleWritable(Double.parseDouble(info[5]));
            context.write(nation,balance);
        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
           double ans=0.0;
           for(DoubleWritable dw:values){
               ans+=dw.get();
           }
           context.write(key,new DoubleWritable(ans));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        //conf.set("fs.defaultFS", "hdfs://localhost:9000");
        if (args.length != 2) {
            System.err.println("Usage: Merge and duplicate removal <in> <out>");
            System.exit(2);
        }
        System.out.println(args[0]);
        System.out.println(args[1]);
        Job job = Job.getInstance(conf, "Main");
        job.setJarByClass(Nation.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
