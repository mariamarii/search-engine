import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CombinerIndex extends Reducer<Text, Text, Text, Text> {
    private final Text fileAtWordFreqValue = new Text();

    public CombinerIndex() {}

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder urls = new StringBuilder();

        for (Text value : values) {
            if (urls.length() > 0) {
                urls.append(";");
            }
            urls.append(value.toString());
        }

        this.fileAtWordFreqValue.set(urls.toString());
        context.write(key, this.fileAtWordFreqValue);
    }
}
