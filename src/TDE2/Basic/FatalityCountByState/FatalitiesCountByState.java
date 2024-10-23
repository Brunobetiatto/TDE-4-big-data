package TDE2.Basic.FatalityCountByState;

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

public class FatalitiesCountByState {


    // Mapper Class
    public static class StateMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final Text state = new Text();
        private final IntWritable fatalities = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Ignorar a linha de cabeçalho
            if (line.startsWith("incident_id")) {
                return;
            }

            // Split usando a vírgula, preservando campos vazios
            String[] fields = line.split(",", -1);

            // Verificar se há ao menos 6 colunas (para capturar o estado e n_killed)
            if (fields.length >= 6) {
                String incidentIdField = fields[0].trim();
                String stateField = fields[2].trim();
                String fatalitiesField = fields[5].trim();

                // Verificar se o incident_id é um número inteiro
                try {
                    Integer.parseInt(incidentIdField);  // Se falhar, a linha será ignorada
                } catch (NumberFormatException e) {
                    System.err.println("Linha ignorada: incident_id não é um número inteiro - " + incidentIdField);
                    return;  // Ignorar a linha
                }

                // Verificar se o campo do estado é válido e se o número de mortes é um número
                if (!fatalitiesField.isEmpty()) {
                    try {
                        int nKilled = Integer.parseInt(fatalitiesField);  // Número de vítimas fatais
                        state.set(stateField);  // Define o estado
                        fatalities.set(nKilled);  // Define o número de vítimas fatais
                        context.write(state, fatalities);  // Emite (estado, número de vítimas fatais)
                    } catch (NumberFormatException e) {
                        System.err.println("Número inválido para vítimas fatais: " + fatalitiesField);
                    }
                } else {
                    System.err.println("Estado inválido ou campo 'n_killed' inválido - " + stateField);
                }
            } else {
                System.err.println("Linha ignorada: Colunas insuficientes - " + line);
            }
        }
    }

    // Reducer Class
    public static class StateReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sumFatalities = 0;

            // Soma o número total de mortes para o estado
            for (IntWritable val : values) {
                sumFatalities += val.get();
            }

            result.set(sumFatalities);
            context.write(key, result);  // Emite (estado, soma de vítimas fatais)
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

        Job job = Job.getInstance(conf, "fatalities count by state");
        job.setJarByClass(FatalitiesCountByState.class);
        job.setMapperClass(StateMapper.class);
        job.setCombinerClass(StateReducer.class);
        job.setReducerClass(StateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
