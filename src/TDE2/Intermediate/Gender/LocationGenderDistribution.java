package TDE2.medium.Gender;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

// Driver, Mapper, Combiner, Reducer tudo no mesmo arquivo
public class LocationGenderDistribution extends Configured implements Tool {

    // Mapper Class
    public static class LocationGenderMapper extends Mapper<LongWritable, Text, LocationGenderWritable, IncidentCountWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columns = line.split(",");

            // Verificar se há dados válidos na cidade e gênero
            if (columns.length > 22 && !columns[3].isEmpty() && !columns[22].isEmpty()) {
                String location = columns[3].trim();  // Campo city_or_county
                String genderField = columns[22].trim();  // Campo participant_gender
                String[] genderEntries = genderField.split("\\|\\|");

                // Emitir chave-valor para cada gênero
                for (String entry : genderEntries) {
                    if (entry.contains("Male")) {
                        context.write(new LocationGenderWritable(location, "Male"), new IncidentCountWritable(1));
                    } else if (entry.contains("Female")) {
                        context.write(new LocationGenderWritable(location, "Female"), new IncidentCountWritable(1));
                    }
                }
            }
        }
    }

    // Combiner Class
    public static class LocationGenderCombiner extends Reducer<LocationGenderWritable, IncidentCountWritable, LocationGenderWritable, IncidentCountWritable> {
        public void reduce(LocationGenderWritable key, Iterable<IncidentCountWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IncidentCountWritable value : values) {
                sum += value.getCount();
            }
            context.write(key, new IncidentCountWritable(sum));
        }
    }

    // Reducer Class
    public static class LocationGenderReducer extends Reducer<LocationGenderWritable, IncidentCountWritable, LocationGenderWritable, IntWritable> {
        public void reduce(LocationGenderWritable key, Iterable<IncidentCountWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IncidentCountWritable value : values) {
                sum += value.getCount();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // Driver Method
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        int reducersQuantity = Integer.parseInt(args[2]);

        // Criando o job
        Job job = Job.getInstance(conf, "LocationGenderCount");
        job.setJarByClass(LocationGenderDistribution.class);
        job.setNumReduceTasks(reducersQuantity);

        // Configurando o caminho de entrada e saída
        FileSystem.get(conf).delete(output, true);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // Configurar Mapper, Combiner, Reducer
        job.setMapperClass(LocationGenderMapper.class);
        job.setCombinerClass(LocationGenderCombiner.class);
        job.setReducerClass(LocationGenderReducer.class);

        job.setMapOutputKeyClass(LocationGenderWritable.class);
        job.setMapOutputValueClass(IncidentCountWritable.class);
        job.setOutputKeyClass(LocationGenderWritable.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new LocationGenderDistribution(), args);
        System.exit(result);
    }
}
