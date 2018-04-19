package com.quyf.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by quyf on 2018/4/17.
 * /opt/modules/hadoop-2.7.2/bin/yarn  jar hdfs_demo.jar com.quyf.hdfs.WordCount
 * MapReduce执行过程 可参考：https://www.cnblogs.com/xia520pi/archive/2012/05/16/2504205.html
 *
 * http://www.cnblogs.com/xia520pi/archive/2012/06/04/2534533.html
 */
public class WordCount {

    //Map类继承自MapReduceBase，并且它实现了Mapper接口，此接口是一个规范类型，它有4种形式的参数，
    // 分别用来指定map的输入key值类型、输入value值类型、输出key值类型和输出value值类型。
    public static class WordCountMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();//每行数据
        //　实现此接口类还需要实现map方法，map方法会具体负责对输入进行操作，
        // 在本例中，map方法对输入的行以空格为单位进行切分，然后使用OutputCollect收集输出的<word,1>。
        @Override
        public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                output.collect(word, one);
            }

        }
    }

    //Reduce类也是继承自MapReduceBase的，需要实现Reducer接口。Reduce类以map的输出作为输入，
    // 因此Reduce的输入类型是<Text，Intwritable>。而Reduce的输出是单词和它的数目，因此，它的输出类型是<Text,IntWritable>。Reduce类也要实现reduce方法，
    // 在此方法中，reduce函数将输入的key值作为输出的key值，然后将获得多个value值加起来，作为输出的值。
    public static class WordCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            result.set(sum);
            output.collect(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        //System.setProperty("HADOOP_USER_NAME","bigdata");
        String input = "/user/bigdata/README.md";
        String output = "/user/bigdata/result";

        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("WordCount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(WordCountMapper.class);
        conf.setCombinerClass(WordCountReducer.class);
        conf.setReducerClass(WordCountReducer.class);
        // 在TextInputFormat中，每个文件（或其一部分）都会单独地作为map的输入,
        // TextInputFormat每行数据都会生成一条记录，每条记录则表示成<key,value>形式
        // key值是每个数据的记录在数据分片中字节偏移量，数据类型是LongWritable；　　
        // value值是每行的内容，数据类型是Text。
        conf.setInputFormat(TextInputFormat.class);
        //每一种输入格式都有一种输出格式与其对应。默认的输出格式是TextOutputFormat，
        // 这种输出方式与输入类似，会将每条记录以一行的形式存入文本文件。
        //不过，它的键和值可以是任意形式的，因为程序内容会调用toString()方法将键和值转换为String类型再输出。
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));
// 运行程序
        System.out.println("running over");
        JobClient.runJob(conf);
        System.exit(0);
    }

}
