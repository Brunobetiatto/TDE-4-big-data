package ExemplosAntonio.Advanced;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

public class EntropyDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new EntropyDriver(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path input = new Path(args[0]);
        Path intermediate = new Path(args[1]);
        Path output = new Path(args[2]);

        int reducers = args.length > 3 ? Integer.parseInt(args[3]) : 2;

        Job job1 = Job.getInstance(conf);
        job1.setJobName("Count-Characters");
        job1.setNumReduceTasks(reducers);

        FileInputFormat.addInputPath(job1,input);
        FileSystem.get(conf).delete(intermediate, true);
        FileOutputFormat.setOutputPath(job1, intermediate);

        //define job1 classes
        job1.setJarByClass(EntropyDriver.class);
        job1.setMapperClass(CharacterCountMapper.class);
        job1.setReducerClass(CharacterCountReducer.class);

        //define output key and value data types for Mapper and Reducer
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        if(job1.waitForCompletion(true)){
            Job job2 = Job.getInstance(conf);
            job2.setJobName("Entropy");

            FileInputFormat.addInputPath(job2,intermediate);
            FileSystem.get(conf).delete(output, true);
            FileOutputFormat.setOutputPath(job2, output);

            //define job2 classes
            job2.setJarByClass(EntropyDriver.class);
            job2.setMapperClass(EntropyMapper.class);
            job2.setReducerClass(EntropyReducer.class);

            // define job2 output key and value data types for Mapper and Reducer
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(EntropyWritable.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoubleWritable.class);

            return job2.waitForCompletion(true) ? 0 : 1;
        }
        return 1;
    }
    public static class CharacterCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String line = value.toString();
            if(!line.startsWith(">")){
                String[] chars = line.split("");
                for(String s:chars){
                    con.write(new Text(s),new IntWritable(1));
                    con.write(new Text("Total"),new IntWritable(1));
                }
            }
        }
    }

    public static class CharacterCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{
            int count = 0;
            for(IntWritable v : values){
                count += v.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static class EntropyMapper extends Mapper<LongWritable, Text, Text, EntropyWritable>{
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] data = line.split("\t");
            String characters = data[0];
            int count = Integer.parseInt(data[1]);
            con.write(new Text("entropy"), new EntropyWritable(characters,count));
        }
    }

    public static class EntropyReducer extends Reducer<Text, EntropyWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<EntropyWritable> values, Context context)
                throws IOException, InterruptedException {
            int total = 0;
            HashMap<String, Integer> data = new HashMap<String, Integer>();
            for(EntropyWritable v : values) data.put(v.getCharacters(), v.getCount());

            total = data.get("Total");
            Set<String> keys = data.keySet();

            for(String k: keys){
                if(!k.equals("Total")){
                    float p = (float) data.get(k)/total;
                    double entropy = -p * Math.log(p);
                    context.write(new Text(k), new DoubleWritable(entropy));
                }
            }
        }
    }
}
