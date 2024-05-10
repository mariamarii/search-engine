import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexPage {

    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text word = new Text();
        private Text linkValue = new Text();
        private Configuration conf;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            conf = context.getConfiguration();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+", 2);
            if (parts.length == 2) {
                String[] links = parts[1].split(";");
                for (String link : links) {
                    word.set(parts[0]);
                    // Get count value using LinkSearch
                    double countValue = 1.0; // Default value if searchLink fails
                    try {
                        // Pass link and link search file path to LinkSearch.searchLink method
                        countValue = Double.parseDouble(Objects.requireNonNull(LinkSearch.searchLink(link, conf.get("link_search_file_path"))));
                        // System.out.println("Result: " + countValue);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    // Emit word, link, and count value as Text
                    linkValue.set(countValue + "*" + link);
                    context.write(word, linkValue);
                    // Debug logging
                    //System.out.println("Mapper Output: " + word.toString() + " -> " + linkValue.toString());
                }
            }
        }

    }

    public static class LinkSortReducer extends Reducer<Text, Text, Text, Text> {

        private final Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> urlCounts = new HashMap<>();

            // Aggregate counts for each URL
            for (Text value : values) {
                String urlWithCount = value.toString();
                int splitIndex = urlWithCount.indexOf("*");
                if (splitIndex != -1) {
                    String countStr = urlWithCount.substring(0, splitIndex);
                    String url = urlWithCount.substring(splitIndex + 1);

                    try {
                        double count = Double.parseDouble(countStr);
                        urlCounts.put(url, count);
                    } catch (NumberFormatException e) {
                        // Handle if count is not a valid integer
                        // Skip this URL
                    }
                }
            }

            // Sort the URLs based on their counts in decreasing order
            List<Map.Entry<String, Double>> sortedUrls = new ArrayList<>(urlCounts.entrySet());
            sortedUrls.sort((a, b) -> b.getValue().compareTo(a.getValue()));

            // Construct the value string with sorted URLs and counts
            StringBuilder urlList = new StringBuilder();
            for (Map.Entry<String, Double> entry : sortedUrls) {
                //System.out.println("aaaaaaaaaaaaaaaaaaaaaaaaaa"+entry.getValue());
                urlList.append(entry.getValue()).append("*").append(entry.getKey()).append(";");
            }

            // Set the output value with the sorted URL list
            result.set(urlList.toString());

            // Emit the key (word) and the concatenated URL list as value
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: InvertedIndexPage <inputPath> <outputPath> <linkSearchFilePath>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("link_search_file_path", args[2]); // Set the link search file path

        Job job = Job.getInstance(conf, "InvertIndexPage");
        job.setJarByClass(InvertedIndexPage.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(LinkSortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
