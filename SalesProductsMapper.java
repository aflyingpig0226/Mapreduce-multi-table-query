package salesProductsMR;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class SalesProductsMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    @Override
    protected void map(LongWritable key1, Text value1, Context context) throws IOException, InterruptedException {
        String pathName = ((FileSplit) context.getInputSplit()).getPath().getName();
        int index = pathName.lastIndexOf("\\");
        String data = value1.toString();
        String[] Infos = data.split(",");
        String path = pathName.substring(index+1);
        if (path.equals("sales")){
            context.write(new IntWritable(Integer.parseInt(Infos[0])), new Text(Infos[2] + ":" + Infos[6]));
        }else {
            context.write(new IntWritable(Integer.parseInt(Infos[0])), new Text("name" + Infos[1]));
        }
    }
}
