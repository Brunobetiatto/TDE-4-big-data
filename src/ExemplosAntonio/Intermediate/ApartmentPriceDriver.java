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

public class ApartmentPriceDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        int reducersQuantity = Integer.parseInt(args[2]);

        Job job = Job.getInstance(conf);
        job.setJobName("ApartmentPriceAverage");
        job.setNumReduceTasks(reducersQuantity);

        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        job.setJarByClass(ApartmentPriceDriver.class);
        job.setMapperClass(ApartmentPriceMapper.class);
        job.setReducerClass(ApartmentPriceReducer.class);
        job.setCombinerClass(ApartmentPriceCombiner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ApartmentPriceWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ApartmentPriceWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        int result = ToolRunner.run(new Configuration(), new ApartmentPriceDriver(), args);

        System.exit(result);
    }

    // Método para operações de agregação
    public static ApartmentPriceWritable operations(Iterable<ApartmentPriceWritable> values) {
        float sumPrice = 0.0f;
        int count = 0;

        for (ApartmentPriceWritable val : values) {
            sumPrice += val.getPrice();
            count++;
        }

        return new ApartmentPriceWritable(sumPrice, count);
    }

    // Mapper
    public static class ApartmentPriceMapper extends Mapper<LongWritable, Text, Text, ApartmentPriceWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columns = line.split(",");
            if (columns.length > 9) {
                String state = columns[9];
                float price = Float.parseFloat(columns[2]);

                context.write(new Text(state), new ApartmentPriceWritable(price, 1));
            }
        }
    }

    // Combiner
    public static class ApartmentPriceCombiner extends Reducer<Text, ApartmentPriceWritable, Text, ApartmentPriceWritable> {
        public void reduce(Text key, Iterable<ApartmentPriceWritable> values, Context context) throws IOException, InterruptedException {
            ApartmentPriceWritable result = operations(values);
            context.write(key, result);
        }
    }

    // Reducer
    public static class ApartmentPriceReducer extends Reducer<Text, ApartmentPriceWritable, Text, ApartmentPriceWritable> {
        public void reduce(Text key, Iterable<ApartmentPriceWritable> values, Context context) throws IOException, InterruptedException {
            ApartmentPriceWritable result = operations(values);
            context.write(key, new ApartmentPriceWritable(result.getPrice() / result.getCount(), result.getCount()));
        }
    }
}
