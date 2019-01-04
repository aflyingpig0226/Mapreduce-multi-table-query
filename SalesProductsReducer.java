package salesProductsMR;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SalesProductsReducer extends Reducer<IntWritable, Text, Text, Text> {
    @Override
    protected void reduce(IntWritable key3, Iterable<Text> values3, Context context) throws IOException, InterruptedException {
        Map<Integer, Double> map = new HashMap<>();
        String name = "";
        for (Text v : values3) {
            String data = v.toString();
            if (data.startsWith("name")){
                name = data.substring(5);
            }else {
                int year = Integer.parseInt(data.substring(0, 4));
                double price = Double.parseDouble(data.split(":")[1]);
                if (map.containsKey(year)){
                    double total = map.get(year);
                    map.put(year, total);
                }else {
                    map.put(year, price);
                }
            }
        }
        context.write(new Text(name), new Text(map.toString()));

    }
}
