import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RecordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text("count");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip header line
        if (key.get() == 0 && value.toString().contains("id")) {
            return;
        }
        context.write(word, one);
    }
}