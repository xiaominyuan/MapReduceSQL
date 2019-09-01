import entity.CombineValues;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


//https://www.cnblogs.com/shsxt/p/8043904.html
//https://www.cnblogs.com/qingyunzong/p/8585170.html

import java.io.IOException;
import java.util.ArrayList;

public class LeftOuterJoin extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeftOuterJoin.class);

    public static class LeftJoinMapper extends Mapper<Object, Text, Text, CombineValues>{

        private CombineValues combineValues = new CombineValues();
        private Text joinKey =  new Text();
        private Text flag = new Text();
        private Text secondPart = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();

            if (pathName.endsWith("city")){
                String[] valueItems = value.toString().split("\\|");
                if (valueItems.length != 5){
                    return;
                }
                flag.set("city");
                joinKey.set(valueItems[0]);
                secondPart.set(valueItems[1] + "\t" + valueItems[2] + "\t" + valueItems[3] + "\t" + valueItems[4]);

                combineValues.setFlag(flag);
                combineValues.setJoinKey(joinKey);
                combineValues.setSecondPart(secondPart);
                context.write(combineValues.getJoinKey(), combineValues.getSecondPart());


            }else if (pathName.endsWith("user")){
                String[] valueItems = value.toString().split("\\|");

                if (valueItems.length != 4){
                    return;
                }

                flag.set("user");
                joinKey.set(valueItems[3]);
                secondPart.set(valueItems[1] + "\t" + valueItems[2] + "\t" + valueItems[3]);

                combineValues.setJoinKey(joinKey);
                combineValues.setSecondPart(secondPart);
                combineValues.setFlag(flag);

                context.write(combineValues.getJoinKey(), combineValues.getSecondPart());
            }
        }
    }

    public static class LeftJoinReducer extends Reducer<Text, CombineValues, Text, Text>{
        private ArrayList<Text> leftTable = new ArrayList<Text>();
        private ArrayList<Text> rightTable = new ArrayList<Text>();
        private Text secondPart = null;
        private Text output = new Text();

        @Override
        protected void reduce(Text key, Iterable<CombineValues> values, Context context) throws IOException, InterruptedException {
            leftTable.clear();
            rightTable.clear();

            for (CombineValues combineValues : values){
                secondPart = new Text(combineValues.getSecondPart().toString());

                if ("city".equals(combineValues.getFlag().toString().trim())){
                    leftTable.add(secondPart);
                }else if ("user".equals(combineValues.getFlag().toString().trim())){
                    rightTable.add(secondPart);
                }
            }

            for (Text left : leftTable){
                for (Text right : rightTable){
                    output.set(left + "\t" + right);
                }
            }

            context.write(key, output);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = new Configuration();
//        JobConf job = new JobConf(configuration);

        Job job = Job.getInstance(configuration);
        job.setJarByClass(LeftOuterJoin.class);

        FileInputFormat.setInputPaths(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        job.setMapperClass(LeftJoinMapper.class);
        job.setReducerClass(LeftJoinReducer.class);

        //mapper 输出的kv 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CombineValues.class);


        //reduce  输出的kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

        return job.isSuccessful() ? 1 : 0;



//        FileInputFormat.setInputPaths(job, new Path(strings[0]));
//        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
//        job.setMapperClass(LeftJoinMapper.class);

    }

    public static void main(String[] args) {
        int returnCode = 0;
        try {
            returnCode = ToolRunner.run(new LeftOuterJoin(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(returnCode);
    }
}
