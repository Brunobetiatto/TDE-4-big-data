package TDE2.Basic.TotalStolenGunsUsedInIncidents;

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

public class StolenGunIncidentsCount {

    // Mapper Class
    public static class StolenGunMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text stolenKey = new Text("stolen");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Ignorar a linha de cabeçalho
            if (line.startsWith("incident_id")) {
                return;
            }

            // Split usando a vírgula, preservando campos vazios
            String[] fields = line.split(",", -1);

            // Verificar se há ao menos 12 colunas (para capturar gun_stolen)
            if (fields.length >= 12) {
                String gunStolenField = fields[11].trim();  // O campo 'gun_stolen' está na 12ª coluna (índice 11)

                // Separar os valores múltiplos em 'gun_stolen'
                String[] gunStatuses = gunStolenField.split("\\|\\|");

                // Verificar se algum dos valores é 'Stolen'
                for (String status : gunStatuses) {
                    if (status.contains("Stolen")) {
                        context.write(stolenKey, one);  // Emite (stolen, 1)
                        return;  // Parar após encontrar 'Stolen'
                    }
                }
            } else {
                System.err.println("Linha ignorada: Colunas insuficientes - " + line);
            }
        }
    }

    // Reducer Class
    public static class StolenGunReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);  // Emite (stolen, total de incidentes com armas roubadas)
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

        Job job = Job.getInstance(conf, "stolen gun incidents count");
        job.setJarByClass(StolenGunIncidentsCount.class);
        job.setMapperClass(StolenGunMapper.class);
        job.setCombinerClass(StolenGunReducer.class);  // Usa o combiner para eficiência
        job.setReducerClass(StolenGunReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
