package ExemplosAntonio.Intermediate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class WindTempDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        int reducersQuantity = Integer.parseInt(args[2]);

        Job job = Job.getInstance(conf);
        job.setJobName("WindTempAggregation");
        job.setNumReduceTasks(reducersQuantity);

        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        job.setJarByClass(WindTempWritable.class);
        job.setMapperClass(WindTempMapper.class);
        job.setReducerClass(WindTempReducer.class);
        job.setCombinerClass(WindTempCombiner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ExemplosAntonio.Intermediate.WindTempWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ExemplosAntonio.Intermediate.WindTempWritable.class);


        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        int result = ToolRunner.run(new Configuration(), new WindTempDriver(), args);

        System.exit(result);
    }

    public static ExemplosAntonio.Intermediate.WindTempWritable operations(Iterable<ExemplosAntonio.Intermediate.WindTempWritable> values){

        float fastestWind = -1000.0f;
        float highestTemp = -1000.0f;

        for (ExemplosAntonio.Intermediate.WindTempWritable val : values) {
            if (val.getWind() > fastestWind) fastestWind = val.getWind();
            if (val.getTemp() > highestTemp) highestTemp = val.getTemp();
        }

      return new ExemplosAntonio.Intermediate.WindTempWritable(fastestWind, highestTemp);
    }

    public static class WindTempMapper extends Mapper<LongWritable, Text, Text, ExemplosAntonio.Intermediate.WindTempWritable>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] collumns = line.split(",");
            if (collumns.length > 0) {
                String month = collumns[2];
                float wind = Float.parseFloat(collumns[10]);
                float temp = Float.parseFloat(collumns[8]);

                context.write(new Text(month), new ExemplosAntonio.Intermediate.WindTempWritable(wind, temp));

            }
        }
    }

    public static class WindTempCombiner extends Reducer<Text, ExemplosAntonio.Intermediate.WindTempWritable, Text, ExemplosAntonio.Intermediate.WindTempWritable> {
        public void reduce(Text key, Iterable<ExemplosAntonio.Intermediate.WindTempWritable> values, Context context) throws IOException, InterruptedException {

            ExemplosAntonio.Intermediate.WindTempWritable result = operations(values);
            context.write(key, result);
        }
    }

    public static class WindTempReducer extends Reducer<Text, ExemplosAntonio.Intermediate.WindTempWritable, Text, ExemplosAntonio.Intermediate.WindTempWritable> {
        public void reduce(Text key, Iterable<ExemplosAntonio.Intermediate.WindTempWritable> values, Context context) throws IOException, InterruptedException {

            ExemplosAntonio.Intermediate.WindTempWritable result = operations(values);
            context.write(key, result);
          }
    }
}


