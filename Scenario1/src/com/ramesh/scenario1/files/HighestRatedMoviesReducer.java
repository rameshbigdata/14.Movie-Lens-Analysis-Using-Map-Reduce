package com.ramesh.ml;

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class HighestRatedMoviesReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
private TreeMap<Double, Text> highestRated = new TreeMap<Double, Text>();
public void reduce(NullWritable key, Iterable<Text> values, Context context)
throws IOException, InterruptedException {
for (Text value : values) {
String data = value.toString();
String[] field = data.split("::", -1);
if (field.length == 2) {
highestRated.put(Double.parseDouble(field[1]), new Text(value));
if (highestRated.size() > 20) {
highestRated.remove(highestRated.firstKey());
}
}
}
for (Text t : highestRated.descendingMap().values()) {
context.write(NullWritable.get(), t);
}
}
}