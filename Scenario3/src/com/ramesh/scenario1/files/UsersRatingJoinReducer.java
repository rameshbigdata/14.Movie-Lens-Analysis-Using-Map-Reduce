package com.ramesh.ml;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class UsersRatingJoinReducer extends Reducer<Text, Text, Text, Text> {
private ArrayList<Text> listUsers = new ArrayList<Text>();
private ArrayList<Text> listRating = new ArrayList<Text>();
@Override
public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
listUsers.clear();
listRating.clear();
for (Text text : values) {
if (text.charAt(0) == 'U') {
listUsers.add(new Text(text.toString().substring(1)));
} else if (text.charAt(0) == 'R') {
listRating.add(new Text(text.toString().substring(1)));
}
}
executeJoinLogic(context);
}
private void executeJoinLogic(Context context) throws IOException, InterruptedException {
if (!listUsers.isEmpty() && !listRating.isEmpty()) {
for (Text usersData : listUsers) {
for (Text ratingData : listRating) {
context.write(usersData, ratingData);
}
}
}
}
}