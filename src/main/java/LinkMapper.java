//import java.io.IOException;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//
//public class LinkMapper extends Mapper<Object, Text, Text, Text> {
//    private Text word = new Text();
//    private Text link = new Text();
//
//    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//        String[] parts = value.toString().trim().split("\\s+", 2);
//        if (parts.length == 2) {
//            String[] links = parts[1].trim().split(";");
//            word.set(parts[0]);
//            for (String l : links) {
//                String[] linkParts = l.split("\\*");
//                if (linkParts.length == 2) {
//                    link.set(linkParts[1]);
//                    context.write(word, link);
//                }
//            }
//        }
//    }
//}
//
//public class LinkReducer extends Reducer<Text, Text, Text, Text> {
//    private Text result = new Text();
//
//    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        StringBuilder links = new StringBuilder();
//        for (Text val : values) {
//            links.append(val.toString()).append("; ");
//        }
//        result.set(links.toString());
//        context.write(key, result);
//    }
//}
//
//public class LinkExtractor {
//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "link extractor");
//        job.setJarByClass(LinkExtractor.class);
//        job.setMapperClass(LinkMapper.class);
//        job.setReducerClass(LinkReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
//}