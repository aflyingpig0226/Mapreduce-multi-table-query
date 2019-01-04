package salesProductsMR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalesProductsMain {
    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(SalesProductsMain.class);

        job.setMapperClass(SalesProductsMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(SalesProductsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\lenovo\\Desktop\\文档\\output\\salesProducts"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\lenovo\\Desktop\\文档\\output\\salesProducts\\result"));

        job.waitForCompletion(true);
    }
}
