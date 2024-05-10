package org.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class GraphConstruction {

    // Mapper class to extract links from web pages
    public static class LinkExtractorMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text page = new Text();
        private Text link = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Parse the input value
            String input = value.toString();
            String content = extractContent(input); // Extract content from XML/HTML
            if (content.isEmpty()) {
                context.getCounter("InvalidInput", input).increment(1); // Log invalid input
                return;
            }

            // Extract links from the content
            List<String> links = extractLinks(content);
            if (links.isEmpty()) {
                context.getCounter("NoLinksFound", content).increment(1); // Log no links found
                return;
            }

            // Emit (currentPage, "!") for each page
            String currentPage = extractCurrentPage(input); // Extract current page from XML/HTML
            if (currentPage.isEmpty()) {
                context.getCounter("NoCurrentPageFound", input).increment(1); // Log no current page found
                return;
            }
            page.set(currentPage);
            context.write(page, new Text("!"));

            // Emit (link, currentPage) for each link
            for (String l : links) {
                link.set(l);
                context.write(link, page);
            }
        }

        // Helper function to extract content from XML/HTML
        private String extractContent(String input) {
            Document doc = Jsoup.parse(input);

            // Extract text content from the parsed document
            String textContent = doc.text();

            return textContent;
        }

        // Helper function to extract links from the content
        private List<String> extractLinks(String content) {
            List<String> links = new ArrayList<>();
            // Regular expression pattern to match links within <a href=""> tags
            Pattern pattern = Pattern.compile("<a\\s+href\\s*=\\s*\"([^\"]+)\"", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(content);
            // Find all occurrences of the pattern
            while (matcher.find()) {
                // Extract the link and add it to the list
                String link = matcher.group(1);
                links.add(link);
            }
            return links;
        }

        // Helper function to extract current page from XML/HTML
        // Helper function to extract current page from XML/HTML
        private String extractCurrentPage(String input) {
            // Implement your logic to extract the current page from XML/HTML
            // For simplicity, let's assume the current page is the value inside <title> tags
            Pattern pattern = Pattern.compile("<title>(.*?)</title>", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(input);
            if (matcher.find()) {
                return matcher.group(1);
            }
            return "";
        }

    }

    public static class GraphConstructionReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder graphRepresentation = new StringBuilder();
            for (Text value : values) {
                graphRepresentation.append(value.toString()).append("\t");
            }
            context.write(key, new Text(graphRepresentation.toString().trim()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "graph construction");
        job.setJarByClass(GraphConstruction.class);
        job.setMapperClass(LinkExtractorMapper.class);
        job.setReducerClass(GraphConstructionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
