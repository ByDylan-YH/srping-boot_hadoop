package com.hadoop.etl;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Author:BYDylan
 * Date:2021/7/2
 * Description:
 */
public class CommonFriendsStepTwoReducer extends Reducer<Text, Text, Text, Text> {
    protected void reduce(Text user_user, Iterable<Text> friends, Context context) throws IOException, InterruptedException {
//        传进来的数据 <用户1- 用户2,好友们>
        StringBuilder stringBuffer = new StringBuilder();
//        遍历所有的好友,并将这些好友放在 stringBuffer 中,以" "分隔
        for (Text friend : friends) {
            stringBuffer.append(friend).append(" ");
        }
//        以好友为 key,用户们为 value 传给下一个 mapper
        context.write(user_user, new Text(stringBuffer.toString()));
    }
}
