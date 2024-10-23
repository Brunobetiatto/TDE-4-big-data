package TDE2.Advanced.MostFrequentlyUsedWeaponByLocation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WeaponAnalysisDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        int exitCode = ToolRunner.run(new Configuration(), new WeaponAnalysisDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Uso: WeaponAnalysisDriver <input path> <intermediate output path> <final output path>");
            System.exit(-1);
        }

        Configuration conf = getConf();

        /** Primeiro Job **/
        Job job1 = Job.getInstance(conf, "Contagem de Armas por Localização");
        job1.setJarByClass(WeaponAnalysisDriver.class);

        Path inputPath = new Path(args[0]);
        Path intermediateOutputPath = new Path(args[1]);

        // Deletar o caminho intermediário se já existir
        FileSystem.get(conf).delete(intermediateOutputPath, true);

        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, intermediateOutputPath);

        job1.setMapperClass(WeaponCountMapper.class);
        job1.setCombinerClass(WeaponCountCombiner.class); // Opcional
        job1.setReducerClass(WeaponCountReducer.class);

        // Definir as classes de chave e valor para o mapeador
        job1.setMapOutputKeyClass(WeaponLocationKey.class);
        job1.setMapOutputValueClass(IntWritable.class);

        // Definir as classes de chave e valor para o resultado final do Job 1
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        boolean success = job1.waitForCompletion(true);
        if (!success) {
            System.err.println("Job 1 falhou, encerrando.");
            return 1;
        }

        /** Segundo Job **/
        Job job2 = Job.getInstance(conf, "Arma Mais Utilizada por Localização");
        job2.setJarByClass(WeaponAnalysisDriver.class);

        Path finalOutputPath = new Path(args[2]);

        // Deletar o caminho final se já existir
        FileSystem.get(conf).delete(finalOutputPath, true);

        FileInputFormat.addInputPath(job2, intermediateOutputPath);
        FileOutputFormat.setOutputPath(job2, finalOutputPath);

        job2.setMapperClass(MaxWeaponMapper.class);
        job2.setReducerClass(MaxWeaponReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(WeaponCountWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        success = job2.waitForCompletion(true);
        if (!success) {
            System.err.println("Job 2 falhou, encerrando.");
            return 1;
        }

        return 0;
    }

    /** Classes Internas do Primeiro Job **/

    // Mapper do Primeiro Job
    public static class WeaponCountMapper extends Mapper<LongWritable, Text, WeaponLocationKey, IntWritable> {
        private WeaponLocationKey outKey = new WeaponLocationKey();
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Ignorar a linha do cabeçalho
            if (key.get() == 0 && value.toString().contains("incident_id")) {
                return;
            }

            String line = value.toString();
            String[] fields = line.split(",", -1); // -1 para incluir campos vazios

            // Verificar se temos campos suficientes e se os dados contêm informações de armas e localizações
            if (fields.length > 12) { // Garantir que temos pelo menos 13 campos (índice 12)
                try {
                    // Índices dos campos (ajuste conforme necessário)
                    int stateIndex = 2; // Coluna 'state' para localização
                    int gunTypeIndex = 12; // Coluna 'gun_type' para o tipo de arma

                    String state = fields[stateIndex].replaceAll("\"", "").trim();
                    String gunTypeStr = fields[gunTypeIndex].replaceAll("\"", "").trim();

                    // Verificação adicional para evitar valores como 'Unknown', 'Unharmed', 'Subject-Suspect' ou lixo
                    if (!state.isEmpty() && !gunTypeStr.isEmpty() && gunTypeStr.matches(".*\\w.*")
                            && !gunTypeStr.contains("Unharmed") && !gunTypeStr.contains("Subject-Suspect")) {
                        // Analisar tipos de armas
                        String[] gunTypes = gunTypeStr.split("\\|\\|");

                        for (String gunEntry : gunTypes) {
                            String[] gunInfo = gunEntry.split("::");
                            if (gunInfo.length == 2) {
                                String weaponType = gunInfo[1].trim();

                                // Configurar a chave personalizada
                                outKey.setLocation(state);
                                outKey.setWeaponType(weaponType);

                                context.write(outKey, one);
                            }
                        }
                    }

                } catch (Exception e) {
                    // Ignorar registros com erros de análise
                    e.printStackTrace();
                }
            }
        }
    }

    // Combiner do Primeiro Job (Opcional)
    public static class WeaponCountCombiner extends Reducer<WeaponLocationKey, IntWritable, WeaponLocationKey, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(WeaponLocationKey key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // Reducer do Primeiro Job
    public static class WeaponCountReducer extends Reducer<WeaponLocationKey, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(WeaponLocationKey key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int total = 0;
            for (IntWritable val : values) {
                total += val.get();
            }
            result.set(total);
            // Escrever a saída como Text com separação por tab
            context.write(new Text(key.toString()), result);
        }
    }

    /** Classes Internas do Segundo Job **/

    // Mapper do Segundo Job
    public static class MaxWeaponMapper extends Mapper<LongWritable, Text, Text, WeaponCountWritable> {

        private Text outKey = new Text();
        private WeaponCountWritable outValue = new WeaponCountWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = line.split("\t");

            if (fields.length == 3) {
                String location = fields[0];
                String weaponType = fields[1];
                int count = Integer.parseInt(fields[2]);

                outKey.set(location);
                outValue.setWeaponType(weaponType);
                outValue.setCount(count);

                context.write(outKey, outValue);
            }
        }
    }

    // Reducer do Segundo Job
    public static class MaxWeaponReducer extends Reducer<Text, WeaponCountWritable, Text, Text> {

        private Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<WeaponCountWritable> values, Context context)
                throws IOException, InterruptedException {

            String maxWeaponType = null;
            int maxCount = 0;
            boolean unknownFound = false;

            for (WeaponCountWritable val : values) {
                // Ignorar o tipo 'Unknown', a menos que não haja outro
                if (!val.getWeaponType().equals("Unknown")) {
                    if (val.getCount() > maxCount) {
                        maxCount = val.getCount();
                        maxWeaponType = val.getWeaponType();
                    }
                } else {
                    unknownFound = true;
                }
            }

            // Se 'Unknown' for a única opção ou ainda for a mais frequente, selecioná-la
            if (maxWeaponType == null && unknownFound) {
                maxWeaponType = "Unknown";
                maxCount = 0; // Ajuste conforme necessário
            }

            if (maxWeaponType != null) {
                result.set(maxWeaponType + "\t" + maxCount);
            } else {
                result.set("N/A\t0");
            }
            context.write(key, result);
        }
    }
}