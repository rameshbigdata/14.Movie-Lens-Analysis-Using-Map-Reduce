package com.ramesh.ml;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class UsersDataMapper extends Mapper<Object, Text, Text, Text> {
private Text userId = new Text();
private Text outvalue = new Text();
private HashMap<Integer, String> profession_mapping = new HashMap<Integer, String>();
private String profession_id_file_location = null;
@Override
public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
String data = values.toString();
String[] field = data.split("::", -1);
if (null != field && field.length == 5 && field[0].length() > 0) {
String profession = profession_mapping.get(Integer.parseInt(field[3]));
String ageRange = AgeStringFactory.getAgeString(field[2]);
if (ageRange != null) {
userId.set(field[0]);
outvalue.set("U" + ageRange + "::" + profession);
context.write(userId, outvalue);
}
}
}
@Override
public void setup(Context context) {
profession_id_file_location = context.getConfiguration().get("id.to.profession.mapping.file.path");
try {
BufferedReader bufferedReader = new BufferedReader(new FileReader(profession_id_file_location));
String line;
while ((line = bufferedReader.readLine()) != null) {
String[] field = line.split(",", -1);
profession_mapping.put(Integer.parseInt(field[0]), field[1]);
}
bufferedReader.close();
} catch (Exception ex) {
System.out.println(ex.getLocalizedMessage());
}
}
}