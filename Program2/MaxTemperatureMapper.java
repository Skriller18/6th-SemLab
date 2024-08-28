import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int MISSING = 9999;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");
        
        if (fields.length > 1) {
            String year = fields[0];
            int temperature;
            try {
                temperature = Integer.parseInt(fields[1]);
            } catch (NumberFormatException e) {
                temperature = MISSING;
            }

            if (temperature != MISSING) {
                context.write(new Text(year), new IntWritable(temperature));
            }
        }
    }
}
