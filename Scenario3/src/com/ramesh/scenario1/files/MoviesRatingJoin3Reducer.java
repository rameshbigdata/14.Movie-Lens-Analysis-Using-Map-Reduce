package com.ramesh.ml;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class MoviesRatingJoin3Reducer extends Reducer<Text, Text, Text, Text> {
private ArrayList<Text> listMovies = new ArrayList<Text>();
private ArrayList<Text> listRating = new ArrayList<Text>();
private Text outvalue=new Text();
@Override
public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
listMovies.clear();
listRating.clear();
for (Text text : values) {
if (text.charAt(0) == 'M') {
listMovies.add(new Text(text.toString().substring(1)));
} else if (text.charAt(0) == 'C') {
listRating.add(new Text(text.toString().substring(1)));
}
}
executeJoinLogic(context);
}
private void executeJoinLogic(Context context) throws IOException, InterruptedException {
if (!listMovies.isEmpty() && !listRating.isEmpty()) {
for (Text moviesData : listMovies) {
String[] data = moviesData.toString().split("::");
String[] genres=data[1].split("\\|");
for (Text ratingData : listRating) {
for (String genre : genres) {
outvalue.set(genre);
context.write(outvalue, ratingData);
}
}
}
}
}
}