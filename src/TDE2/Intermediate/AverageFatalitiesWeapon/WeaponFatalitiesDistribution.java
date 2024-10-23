package TDE2.medium.AverageFatalitiesWeapon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
public class WeaponFatalitiesDistribution extends Configured implements Tool {

    // Mapper Class
    public static class WeaponMapper extends Mapper<LongWritable, Text, WeaponTypeWritable, SumCountWritable> {
        private boolean isFirstLine = true;  // Para ignorar a primeira linha (cabeçalho)

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columns = line.split(",");

            // Verificar se a linha não é o cabeçalho e se há dados válidos para tipo de arma e vítimas fatais
            if (isFirstLine) {
                // Ignorar a primeira linha
                isFirstLine = false;
                return;
            }

            if (columns.length > 12 && !columns[12].isEmpty() && !columns[5].isEmpty()) {
                try {
                    String weaponField = columns[12].trim();  // Campo gun_type (13ª coluna)
                    int fatalities = Integer.parseInt(columns[5].trim());  // Campo n_killed (6ª coluna)

                    // Separar os tipos de armas por '||'
                    String[] weaponTypes = weaponField.split("\\|\\|");

                    // Emitir uma chave-valor para cada tipo de arma
                    for (String weaponEntry : weaponTypes) {
                        // Extrair o nome da arma (depois de "::")
                        String[] weaponData = weaponEntry.split("::");
                        if (weaponData.length == 2) {
                            String weaponType = weaponData[1].trim();
                            context.write(new WeaponTypeWritable(weaponType), new SumCountWritable(fatalities, 1));
                        }
                    }
                } catch (NumberFormatException e) {
                    // Ignorar a linha se n_killed não for um número válido
                    System.err.println("Erro ao processar n_killed: " + columns[5]);
                }
            }
        }
    }

    // Combiner Class
    public static class WeaponCombiner extends Reducer<WeaponTypeWritable, SumCountWritable, WeaponTypeWritable, SumCountWritable> {
        public void reduce(WeaponTypeWritable key, Iterable<SumCountWritable> values, Context context) throws IOException, InterruptedException {
            int sumFatalities = 0;
            int count = 0;
            for (SumCountWritable value : values) {
                sumFatalities += value.getSum();
                count += value.getCount();
            }
            context.write(key, new SumCountWritable(sumFatalities, count));
        }
    }

    // Reducer Class
    public static class WeaponReducer extends Reducer<WeaponTypeWritable, SumCountWritable, WeaponTypeWritable, FloatWritable> {
        public void reduce(WeaponTypeWritable key, Iterable<SumCountWritable> values, Context context) throws IOException, InterruptedException {
            int sumFatalities = 0;
            int count = 0;
            for (SumCountWritable value : values) {
                sumFatalities += value.getSum();
                count += value.getCount();
            }
            // Calcula a média de vítimas fatais por tipo de arma
            float averageFatalities = (float) sumFatalities / count;
            context.write(key, new FloatWritable(averageFatalities));
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
        Job job = Job.getInstance(conf, "WeaponFatalitiesAverage");
        job.setJarByClass(WeaponFatalitiesDistribution.class);
        job.setNumReduceTasks(reducersQuantity);

        // Configurando o caminho de entrada e saída
        FileSystem.get(conf).delete(output, true);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // Configurar Mapper, Combiner, Reducer
        job.setMapperClass(WeaponMapper.class);
        job.setCombinerClass(WeaponCombiner.class);
        job.setReducerClass(WeaponReducer.class);

        job.setMapOutputKeyClass(WeaponTypeWritable.class);
        job.setMapOutputValueClass(SumCountWritable.class);
        job.setOutputKeyClass(WeaponTypeWritable.class);
        job.setOutputValueClass(FloatWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new WeaponFatalitiesDistribution(), args);
        System.exit(result);
    }
}
