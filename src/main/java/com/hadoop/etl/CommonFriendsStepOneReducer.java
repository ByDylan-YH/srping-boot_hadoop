package com.hadoop.etl;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Author:BYDylan
 * Date:2021/7/2
 * Description:
 */
public class CommonFriendsStepOneReducer extends Reducer<Text, Text, Text, Text> {
    protected void reduce(Text friend, Iterable<Text> users, Context context) throws IOException, InterruptedException {
//        传进来的数据<好友,用户>: <B,A>,<C,A>,<D,A>,<A,B>,<C,B>,<E,B>,<K,B>......
        StringBuilder stringBuffer = new StringBuilder();
//        遍历所有的用户,并将用户放在 stringBuffer 中,以","分隔
        for (Text user : users) {
            stringBuffer.append(user).append(",");
        }
//        以好友为 key,用户们为 value 传给下一个 mapper
        context.write(friend, new Text(stringBuffer.toString()));
    }
}