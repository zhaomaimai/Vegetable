import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class VegetablePriceReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private Text locationDateKey = new Text();
    private DoubleWritable maxPrice = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double max = Double.MIN_VALUE;

        // Finding the maximum price for a specific location, date, and vegetable
        for (DoubleWritable value : values) {
            max = Math.max(max, value.get());
        }

        // Emitting the key-value pair with the location, date, and maximum price
        locationDateKey.set(key.toString());
        maxPrice.set(max);

        context.write(locationDateKey, maxPrice);
    }
}
