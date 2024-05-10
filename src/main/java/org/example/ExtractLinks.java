package org.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.example.DecodingFileName.fileNameToUrl;

public class ExtractLinks {

    public static class WholeFileInputFormat extends TextInputFormat {
        @Override
        protected boolean isSplitable(JobContext context, Path file) {
            return false; // Prevent splitting files
        }


}

    public static class LinkMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text filename = new Text();
        private Text links = new Text();
        private static int fileCount = 1;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String filename = "File" + fileCount++;
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            fileName=fileNameToUrl(fileName);
            this.filename.set(fileName);

            String htmlContent = value.toString();
            List<String> linkList = extractLinks(htmlContent,fileName);
            if (!linkList.isEmpty())
            links.set(String.join(",", linkList));
            context.write(this.filename, links);
        }

        private List<String> extractLinks(String htmlContent, String fileName) {
            List<String> links = new ArrayList<>();
            Document doc = Jsoup.parse(htmlContent);
            if (doc != null) {
                Elements linkElements = doc.select("a[href]");
                for (Element link : linkElements) {
                    String href = link.attr("href").trim();
                    if (!href.startsWith("javascript:void(0)") && !href.equals("#content")) {
                        // If the link starts with '/', concatenate it with the file name
                        if (href.startsWith("/")) {
                            href = fileName + href;
                        }
                        links.add(href);
                    }
                }
            }
            return links;
        }

    }


        public static class LinkReducer extends Reducer<Text, Text, Text, Text> {

            @Override
            protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
                StringBuilder linksBuilder = new StringBuilder();

                // Append the key followed by a tab character
                linksBuilder.append(key.toString()).append("\t");

                // Append the values separated by commas
                for (Iterator<Text> iterator = values.iterator(); iterator.hasNext();) {
                    Text value = iterator.next();
                    if (linksBuilder.length() > key.getLength() + 1) {
                        // Add a comma before appending the next value, if it's not the first value
                        linksBuilder.append(",");
                    }
                    linksBuilder.append(value.toString());
                }

                context.write(new Text(linksBuilder.toString()), null); // Write the key-value pair
            }

        }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Extract Links");
        job.setJarByClass(ExtractLinks.class);
        job.setInputFormatClass(WholeFileInputFormat.class); // Use custom input format
        job.setMapperClass(LinkMapper.class);
        job.setReducerClass(LinkReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
