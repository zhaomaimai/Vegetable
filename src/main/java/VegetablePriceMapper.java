import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VegetablePriceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text locationDateKey = new Text();
    private DoubleWritable priceValue = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0) {
            // Skip the header line
            return;
        }

        String[] tokens = value.toString().split("\t");

        // Assuming the date is in the format "YYYY年MM月DD日"
        String date = tokens[2].replaceAll("[年月]", "-").replace("日", "");

        String vegetable = tokens[1];
        String location = tokens[7];
        double price = Double.parseDouble(tokens[3]);

        // Emitting the key-value pair with the location, date, and price
        locationDateKey.set(location + "_" + date + "_" + vegetable);
        priceValue.set(price);

        context.write(locationDateKey, priceValue);
    }
}
