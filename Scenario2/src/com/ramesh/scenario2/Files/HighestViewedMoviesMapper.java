package com.ramesh.ml;

import java.io.IOException;
import java.util.TreeMap;
import java.util.Map;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

 
public class HighestViewedMoviesMapper extends Mapper<Object, Text, NullWritable, Text>  
{
	
	private TreeMap<Integer, Text> highestView = new TreeMap<Integer, Text>();

@Override
public void map(Object key,Text values,Context context) throws IOException,InterruptedException
{
	String data = values.toString();
	String[] field = data.split("::", -1);
	if(null != field && field.length==2)
	{
	highestView.put(Integer.parseInt(field[1]), new Text(field[0]+"::"+field[1]));

		if (highestView.size() >10)
		{
			highestView.remove(highestView.firstKey());
		}
	}
}
@Override
protected void cleanup(Context context) throws IOException, InterruptedException {
for (Map.Entry<Integer, Text> entry : highestView.entrySet()) {
context.write(NullWritable.get(), entry.getValue());
}
}
}
