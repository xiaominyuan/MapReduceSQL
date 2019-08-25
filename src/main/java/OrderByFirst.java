import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.Iterator;

//求每个部门最早进入公司的员工姓名

public class OrderByFirst {
    public static class orderByMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text>{

        @Override
        public void map(Object o, Text text, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            try {
                Emp emp = new Emp(text.toString());
                Text key = new Text(emp.getDeptNo());
                Text value = new Text(emp.getHireDate()+ "~" + emp.getName());

                outputCollector.collect(key, value);
            }catch (Exception e){
                throw e;
            }
        }

    }


    public static class orderByReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce(Text text, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

            DateFormat dateFormat = DateFormat.getDateInstance();
            Date minDate = new Date(9999,12,30);

            Date nowDate;

            String[] dateAndName = null;

            while (iterator.hasNext()){
                dateAndName = iterator.next().toString().split("~");
                try {
                    nowDate = dateFormat.parse(dateAndName[0].toString().substring(0,10));

                    if (nowDate.before(minDate)){
                        minDate = nowDate;
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }

            }

            Text value = new Text(minDate.toLocaleString() + " " + dateAndName[1]);

            outputCollector.collect(text, value);
        }
    }

    public static void main(String[] args) {
        JobConf conf = new JobConf(OrderByFirst.class);
        conf.setJobName("order");

        //设置mapper输出的k-v类型
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(orderByMapper.class);
        conf.setReducerClass(orderByReduce.class);
        conf.setCombinerClass(orderByReduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path("C:\\Users\\m\\Desktop\\emp.txt"));
        FileOutputFormat.setOutputPath(conf, new Path("C:\\Users\\m\\Desktop\\orderFirst"));

        try {
            JobClient.runJob(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
