
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class FindCommonFriendOne {

    public static class FindFriendMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        // 泛型，定义输入输出的类型
        /**
         * 友  人
         */
        Text text = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 将mptask传给我们的文本内容转换成String
            String line = value.toString();
            IntWritable ONE = new IntWritable(1);
            // 根据空格切分
            String[] qqAndFriend = line.split(":");//分割出QQ号
            String qq = qqAndFriend[0];
            String otherFriend = "";
            StringBuffer friendbuf = new StringBuffer(qqAndFriend[1]+",");

            String[] friends = qqAndFriend[1].split(",");
            for (String friend : friends) {
                //查找其他朋友
                //otherFriend = friendbuf.delete(friendbuf.indexOf(friend),friendbuf.indexOf(friend)+1).toString();
                context.write(new Text(friend), new Text(qq));
            }
        }
    }


    public static class FindFriendReducer extends
            Reducer<Text, Text, Text, Text> {


        @Override
        protected void reduce(Text Keyin, Iterable<Text> values,
                              Context context) throws IOException, InterruptedException {
            String qqs = "";
            for (Text val : values) {
                qqs +=val.toString() + ",";
            }
            context.write(Keyin, new Text(qqs));
        }
    }


    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(FindCommonFriendOne.class);

        job.setMapperClass(FindFriendMapper.class);
        job.setReducerClass(FindFriendReducer.class);
        //指定最终输出的数据kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 :1);
    }

}