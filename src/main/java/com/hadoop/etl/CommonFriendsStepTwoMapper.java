package com.hadoop.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * Author:BYDylan
 * Date:2021/7/2
 * Description:
 */
public class CommonFriendsStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
//        传进来的数据A I,K,C,B,G,F,H,O,D,转换成字符串并以"\t"分割,得到fiendsAndUsers
        String[] friendAndUsers = value.toString().split("\t");
//        fiendsAndUsers 0 位置为好友 "A"
        String friend = friendAndUsers[0];
//        fiendsAndUsers 1 位置为拥有上面好友用户们以 ","进行分割字符串,得到每一个用户,{"I","K",....}
        String[] users = friendAndUsers[1].split(",");
//        将 user 进行排序,避免重复
        Arrays.sort(users);
//        以用户-用户为 key,好友们做 value 传给reducer(冒泡排序)
        for (int i = 0; i < users.length - 2; i++) {
            for (int j = i + 1; j < users.length - 1; j++) {
                context.write(new Text(users[i] + "-" + users[j]), new Text(friend));
            }
        }
    }
}