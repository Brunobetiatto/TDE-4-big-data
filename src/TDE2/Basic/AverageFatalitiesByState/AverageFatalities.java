package TDE2.Basic.AverageFatalitiesByState;

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

public class AverageFatalities {

    // Mapper Class
    public static class FatalitiesMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static Text usa = new Text("USA");
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
                String fatalitiesField = fields[5].trim();

                // Verificar se o incident_id é um número inteiro
                try {
                    Integer.parseInt(incidentIdField);  // Se falhar, a linha será ignorada
                } catch (NumberFormatException e) {
                    System.err.println("Linha ignorada: incident_id não é um número inteiro - " + incidentIdField);
                    return;  // Ignorar a linha
                }

                // Verificar se o campo de mortes (n_killed) é válido
                if (!fatalitiesField.isEmpty()) {
                    try {
                        int nKilled = Integer.parseInt(fatalitiesField);  // Número de vítimas fatais
                        fatalities.set(nKilled);  // Define o número de vítimas fatais
                        context.write(usa, fatalities);  // Emite ("USA", número de vítimas fatais)
                    } catch (NumberFormatException e) {
                        System.err.println("Número inválido para vítimas fatais: " + fatalitiesField);
                    }
                }
            }
        }
    }

    // Reducer Class
    public static class AverageReducer extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sumFatalities = 0;
            int countIncidents = 0;

            // Soma o número total de mortes e conta o número de incidentes
            for (IntWritable val : values) {
                sumFatalities += val.get();
                countIncidents++;
            }

            // Calcular a média (evitar divisão por zero)
            double averageFatalities = countIncidents > 0 ? (double) sumFatalities / countIncidents : 0.0;

            // Formatar o resultado com 2 casas decimais
            String formattedAverage = String.format("%.2f", averageFatalities);

            context.write(new Text("Average Fatalities in the USA"), new Text(formattedAverage));  // Emite ("Average Fatalities", média formatada)
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

        Job job = Job.getInstance(conf, "average fatalities");
        job.setJarByClass(AverageFatalities.class);
        job.setMapperClass(FatalitiesMapper.class);
        job.setReducerClass(AverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
