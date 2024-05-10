import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.conf.Configuration; // Import Configuration

public class LinkSearch {

    public static String searchLink(String link, String inputPath) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "link search");
        job.setJarByClass(LinkSearch.class);
        job.setMapperClass(LinkMapper.class);
        job.setReducerClass(LinkReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the link and input path as configuration parameters
        job.getConfiguration().set("search_link", link);
        job.getConfiguration().set("link_search_file_path", inputPath);

        // Set the output format to NullOutputFormat to discard any output
        job.setOutputFormatClass(NullOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));

        // Wait for the job to complete
        if (job.waitForCompletion(true)) {
            // Retrieve the result from the reducer
            return LinkReducer.getResult();
        } else {
            return null;
        }
    }


    public static class LinkMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValue = new Text();
        private boolean linkFound = false;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Get the search link from the configuration
            String searchLink = context.getConfiguration().get("search_link");

            // Split the line into link and value
            String[] parts = value.toString().split("\\s+", 2);

            // Check if there are at least two parts (link and value)
            if (parts.length == 2) {
                String link = parts[0];
                String linkValue = parts[1];
                //System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAaa"+link);
               //System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAaa"+linkValue);

                // Emit the link and its value only if it matches the search link and hasn't been emitted before
                if (!linkFound && link.equals(searchLink)) {
                   // System.out.println("link:" +searchLink);

                   // System.out.println("link value" +linkValue);
                    outputKey.set(link);
                    outputValue.set(linkValue);


                    context.write(outputKey, outputValue);
                    linkFound = true;
                }
            }
        }
    }


    public static class LinkReducer extends Reducer<Text, Text, Text, Text> {
        private static Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Emit only the value associated with the searched link
            Text firstValue = values.iterator().next();
           // System.out.println("first value" +firstValue);
            result.set(firstValue);
            //System.out.println(result);

            context.write(new Text(), result);
        }

        // Inside your LinkReducer class
        public static String getResult() {


            // Convert the result to a double
            double value = Double.parseDouble(result.toString());
            // Format the double to a string with desired precision
            DecimalFormat df = new DecimalFormat("#.#################"); // specify the desired precision
            return df.format(value);
        }
    }




}