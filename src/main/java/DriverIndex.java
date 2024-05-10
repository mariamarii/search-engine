import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.example.DecodingFileName;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class DriverIndex {

    public static class MapperIndex extends Mapper<LongWritable, Text, Text, Text> {
        private Text keyInfo = new Text();

        public MapperIndex() {}

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            String filename = split.getPath().getName();
            filename = DecodingFileName.fileNameToUrl(filename);
            Document doc = Jsoup.parse(value.toString());
            StringBuilder contentBuilder = new StringBuilder();
            doc.select("p, h1, h2, h3, h4, h5, h6, title").forEach((element) -> {
                contentBuilder.append(element.text()).append(" ");
            });
            String text = contentBuilder.toString().trim();
            if (!text.isEmpty()) {
                String[] words = text.split("\\s+");
                for (String word : words) {
                    if (!word.isEmpty()) {
                        keyInfo.set(word);
                        context.write(keyInfo, new Text(filename));
                    }
                }
            }
        }
    }


    public static class ReducerIndex extends Reducer<Text, Text, Text, Text> {
        private final Text result = new Text();

        public ReducerIndex() {}

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder fileList = new StringBuilder();

            for (Text value : values) {
                if (fileList.length() > 0) {
                    fileList.append(";");
                }
                fileList.append(value.toString());
            }

            result.set(fileList.toString());
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: IndexJob <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Index");

        job.setJarByClass(DriverIndex.class);
        job.setMapperClass(MapperIndex.class);
        job.setReducerClass(ReducerIndex.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
