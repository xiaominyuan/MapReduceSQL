import javafx.beans.binding.DoubleExpression;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.Iterator;

public class Avg {
    public static class avgMap extends MapReduceBase implements Mapper<Object, Text, Text, DoubleWritable>{

        //Mapper<Object, Text, Text, DoubleWritable> 前面两个参数是输入的数据类型， 后面两个参数是mapper 输出的数据类型

        @Override
        public void map(Object o, Text text, OutputCollector<Text, DoubleWritable> outputCollector, Reporter reporter) throws IOException {
            try {
                Emp emp = new Emp(text.toString());
                //key
                Text key = new Text(emp.getDeptNo());

                //value
                DoubleWritable value = new DoubleWritable(Double.parseDouble(emp.getSalary()));

                outputCollector.collect(key, value);
            }catch (Exception e){
                throw e;
            }
        }
    }

    public static class avgReduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable>{

        //Reducer<Text, DoubleWritable, Text, DoubleWritable>
        //前面两个参数是mappper输入的数据类型， 后面两个参数是reducer 输出的数据类型

        @Override
        public void reduce(Text text, Iterator<DoubleWritable> iterator, OutputCollector<Text, DoubleWritable> outputCollector, Reporter reporter) throws IOException {
            Double sum = 0.0;
            int count = 0;

            while (iterator.hasNext()){
                sum = sum + iterator.next().get();
                count++;
            }


            outputCollector.collect(text, new DoubleWritable(sum/count));
        }
    }

    public static void main(String[] args) {
        JobConf jobConf = new JobConf(Avg.class);
        jobConf.setJobName("average");

        //mappper 输出的类型
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(DoubleWritable.class);

        jobConf.setMapperClass(avgMap.class);
        jobConf.setCombinerClass(avgReduce.class);
        jobConf.setReducerClass(avgReduce.class);

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(jobConf, new Path("C:\\Users\\m\\Desktop\\emp.txt"));
        FileOutputFormat.setOutputPath(jobConf, new Path("C:\\Users\\m\\Desktop\\avg"));

        try {
            JobClient.runJob(jobConf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
