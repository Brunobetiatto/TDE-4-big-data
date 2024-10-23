package TDE2.Intermediate.AverageAgeByWeaponAndLocation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AverageAgeByWeaponAndLocation extends Configured implements Tool {

    public static class MapperClass extends Mapper<LongWritable, Text, WeaponLocationKey, AgeCountWritable> {

        private WeaponLocationKey outKey = new WeaponLocationKey();
        private AgeCountWritable outValue = new AgeCountWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Ignorar a linha do cabeçalho
            if (key.get() == 0 && value.toString().contains("incident_id")) {
                return;
            }

            String line = value.toString();
            String[] fields = line.split(",", -1); // -1 para incluir campos vazios

            // Verificar se temos campos suficientes
            if (fields.length > 19) { // Garantir que temos pelo menos 20 campos (índice 19)
                try {
                    // Índices dos campos (ajuste conforme necessário)
                    int stateIndex = 2; // Coluna 'state'
                    int participantAgeIndex = 19; // Coluna 'participant_age'
                    int gunTypeIndex = 12; // Coluna 'gun_type'

                    String state = fields[stateIndex].replaceAll("\"", "").trim();
                    String participantAgeStr = fields[participantAgeIndex].replaceAll("\"", "").trim();
                    String gunTypeStr = fields[gunTypeIndex].replaceAll("\"", "").trim();

                    // Analisar idades dos participantes
                    String[] participantAges = participantAgeStr.split("\\|\\|");
                    // Analisar tipos de armas
                    String[] gunTypes = gunTypeStr.split("\\|\\|");

                    for (String ageEntry : participantAges) {
                        String[] ageInfo = ageEntry.split("::");
                        if (ageInfo.length == 2) {
                            String ageStr = ageInfo[1];
                            // Verificar se a idade é numérica
                            if (ageStr.matches("\\d+")) {
                                int age = Integer.parseInt(ageStr);

                                // Para cada tipo de arma
                                for (String gunEntry : gunTypes) {
                                    String[] gunInfo = gunEntry.split("::");
                                    if (gunInfo.length == 2) {
                                        String gunType = gunInfo[1];

                                        // Definir a chave personalizada
                                        outKey.setWeaponType(gunType);
                                        outKey.setState(state);
                                        outValue.setAgeSum(age);
                                        outValue.setCount(1);

                                        context.write(outKey, outValue);
                                    }
                                }
                            }
                        }
                    }

                } catch (Exception e) {
                    // Ignorar registros com erros de análise
                }
            }
        }
    }

    public static class CombinerClass extends Reducer<WeaponLocationKey, AgeCountWritable, WeaponLocationKey, AgeCountWritable> {
        private AgeCountWritable result = new AgeCountWritable();

        @Override
        protected void reduce(WeaponLocationKey key, Iterable<AgeCountWritable> values, Context context)
                throws IOException, InterruptedException {
            int sumAges = 0;
            int count = 0;
            for (AgeCountWritable val : values) {
                sumAges += val.getAgeSum();
                count += val.getCount();
            }
            result.setAgeSum(sumAges);
            result.setCount(count);
            context.write(key, result);
        }
    }

    public static class ReducerClass extends Reducer<WeaponLocationKey, AgeCountWritable, WeaponLocationKey, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        protected void reduce(WeaponLocationKey key, Iterable<AgeCountWritable> values, Context context)
                throws IOException, InterruptedException {
            int sumAges = 0;
            int count = 0;
            for (AgeCountWritable val : values) {
                sumAges += val.getAgeSum();
                count += val.getCount();
            }
            double averageAge = (double) sumAges / count;
            result.set(averageAge);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        int exitCode = ToolRunner.run(new Configuration(), new AverageAgeByWeaponAndLocation(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Uso: AverageAgeByWeaponAndLocation <caminho de entrada> <caminho de saída>");
            System.exit(-1);
        }

        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "Média de Idade por Tipo de Arma e Localização");
        job.setJarByClass(AverageAgeByWeaponAndLocation.class);

        // Definir caminhos de entrada e saída
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        // Deletar o caminho de saída se já existir
        FileSystem.get(conf).delete(outputPath, true);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Configurar classes Mapper, Combiner e Reducer
        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);

        // Definir tipos de saída
        job.setMapOutputKeyClass(WeaponLocationKey.class);
        job.setMapOutputValueClass(AgeCountWritable.class);

        job.setOutputKeyClass(WeaponLocationKey.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Executar o job
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
