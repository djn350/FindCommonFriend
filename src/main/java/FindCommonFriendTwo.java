

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

public class FindCommonFriendTwo {

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
            String[] friendAndQQ = line.split("\t");//分割出QQ号
            String friend = friendAndQQ[0];
            String otherFriend = "";
            StringBuffer friendbuf = new StringBuffer(friendAndQQ[1] );

            String[] qqs = friendAndQQ[1].split(",");
            for (int i=0;i < qqs.length;i++) {
                //查找其他朋友
                for(int j = i+1;j<qqs.length;j++)
                {
                    //避免出现A-D 与D-A的情况
                    if(qqs[i].compareTo(qqs[j])>0)
                    {
                        context.write(new Text(qqs[i]+"-"+qqs[j]), new Text(friend));
                    }
                    else{
                        context.write(new Text(qqs[j]+"-"+qqs[i]), new Text(friend));
                    }

                }


            }
        }
    }


    public static class FindFriendReducer extends
            Reducer<Text, Text, Text, Text> {


        @Override
        protected void reduce(Text Keyin, Iterable<Text> values,
                              Context context) throws IOException, InterruptedException {
            StringBuffer friends = new StringBuffer();
            for (Text val : values) {
                if(friends.indexOf(val.toString())<0)
                {
                    friends.append(val).append(",");
                }
            }
            context.write(Keyin, new Text(friends.toString()));
        }
    }


    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(FindCommonFriendTwo.class);

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
