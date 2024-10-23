package TDE2.Basic.IncidentCountByState;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;


public class IncidentCountByState {

    // Mapper Class
    public static class StateMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text state = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Ignorar a linha de cabeçalho
            if (line.startsWith("incident_id")) {
                return;
            }

            // Split usando a vírgula, preservando campos vazios
            String[] fields = line.split(",", -1);

            // Verificar se há ao menos 3 colunas
            if (fields.length >= 3) {
                String stateField = fields[2].trim();
                String incidentIdField = fields[0].trim();
                try {
                    Integer.parseInt(incidentIdField);  // Se falhar, a linha será ignorada
                } catch (NumberFormatException e) {
                    System.err.println("Linha ignorada: incident_id não é um número inteiro - " + incidentIdField);
                    return;  // Ignorar a linha
                }
                state.set(stateField);  // Define o estado
                context.write(state, one);

            } else {
                System.err.println("Linha ignorada: Colunas insuficientes - " + line);
            }
        }
    }

    // Reducer Class
    public static class StateReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);  // Define a soma total para aquele estado
            context.write(key, result);  // Emite (estado, soma) como saída
        }
    }

    // Main Method
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();

        // Obtém o sistema de arquivos
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(args[1]);

        // Verifica se o diretório de saída já existe
        if (fs.exists(outputPath)) {
            // Se existir, deleta o diretório
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "incident count by state");
        job.setJarByClass(IncidentCountByState.class);
        job.setMapperClass(StateMapper.class);
        job.setCombinerClass(StateReducer.class);  // Usa o combiner para eficiência
        job.setReducerClass(StateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));  // Diretório de entrada
        FileOutputFormat.setOutputPath(job, outputPath);  // Diretório de saída

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
