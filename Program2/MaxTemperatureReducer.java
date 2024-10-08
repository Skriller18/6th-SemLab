import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxTemperature = Integer.MIN_VALUE;

        for (IntWritable value : values) {
            maxTemperature = Math.max(maxTemperature, value.get());
        }

        context.write(key, new IntWritable(maxTemperature));
    }
}
