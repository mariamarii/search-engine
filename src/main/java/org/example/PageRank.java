package org.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRank {

    public static class PageRankMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            String page = parts[0];
            String[] links = parts[1].split(",");

            // Emit key-value pairs for outgoing links
            for (String link : links) {
                context.write(new Text(link), new DoubleWritable(1.0 / links.length));
            }

            // Emit key-value pair for the page itself with value 0.0
            context.write(new Text(page), new DoubleWritable(0.0));
        }
    }

    public static class PageRankReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;

            // Sum up the ranks contributed by incoming links
            for (DoubleWritable value : values) {
                sum += value.get();
            }

            double dampingFactor = 0.85; // Damping factor (usually set to 0.85 in PageRank)
            double newRank = (1 - dampingFactor) + dampingFactor * sum;

            context.write(key, new DoubleWritable(newRank));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PageRank");
        job.setJarByClass(PageRank.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
