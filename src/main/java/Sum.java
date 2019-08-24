import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Iterator;

public class Sum {

    public static class sumMap extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable>{
        @Override
        public void map(Object o, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {



            try {
                Emp emp = new Emp(text.toString());

                //key 员工部门号
                Text key = new Text(emp.getDeptNo());

                //value 员工薪水
                IntWritable value = new IntWritable(Integer.parseInt(emp.getSalary()));

                outputCollector.collect(key, value);
            }catch (Exception e){
                reporter.getCounter(ErrCount.LINKSKIP).increment(1);
                throw e;
            }
        }
    }

    public static class sumReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            int sum = 0;
            while (iterator.hasNext()){
                sum = sum + iterator.next().get();
            }

            outputCollector.collect(text, new IntWritable(sum));
        }
    }

    public static void main(String[] args) {
        JobConf conf = new JobConf(Sum.class);
        conf.setJobName("sum");

        //设置mapper输出的k-v类型
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(sumMap.class);
        conf.setReducerClass(sumReduce.class);
        conf.setCombinerClass(sumReduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path("C:\\Users\\m\\Desktop\\emp.txt"));
        FileOutputFormat.setOutputPath(conf, new Path("C:\\Users\\m\\Desktop\\sum1"));

        try {
            JobClient.runJob(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
