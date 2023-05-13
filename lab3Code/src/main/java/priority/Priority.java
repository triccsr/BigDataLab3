package priority;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class Priority {

    public static class Map extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] info=value.toString().split("\\|");
            String orderKey=info[0];
            Text orderPriority=new Text(info[5]);
            String shipPriority=info[7];
            Text msg= new Text(shipPriority+"\t"+orderKey);
            context.write(orderPriority,msg);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int maxShip=-99999;
            ArrayList<Text> maxList=new ArrayList<>();
            for(Text v : values){
                String[] s=v.toString().split("\t");
                int shipPriority=Integer.parseInt(s[0]);
                if(shipPriority>maxShip){
                    maxShip=shipPriority;
                    maxList.clear();
                    maxList.add(new Text(v));
                }
                else if(shipPriority==maxShip){
                    maxList.add(new Text(v));
                }
            }
            for(Text o:maxList){
                context.write(key,o);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        //conf.set("fs.defaultFS", "hdfs://master:9000");
        if (args.length != 2) {
            System.err.println("Usage: Merge and duplicate removal <in> <out>");
            System.exit(2);
        }
        System.out.println(args[0]);
        System.out.println(args[1]);
        Job job = Job.getInstance(conf, "Main");
        job.setJarByClass(priority.Priority.class);
        job.setMapperClass(priority.Priority.Map.class);
        job.setReducerClass(priority.Priority.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
