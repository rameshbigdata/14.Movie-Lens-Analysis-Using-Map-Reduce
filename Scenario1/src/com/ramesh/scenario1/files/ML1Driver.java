package com.ramesh.ml;
 

	import java.io.File;
	import java.io.IOException;
	import org.apache.commons.io.FileUtils;
	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.NullWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
	import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
	
	
	public class ML1Driver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	/*
	* I have used my local path in windows change the path as per your
	* local machine
	*/
	/*
	* I have used my local path in windows change the path as per your
	* local machine
	*/
	args = new String[] { 
	"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/Movie_Lens_Project/ml-1m/movies.dat",
	"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/Movie_Lens_Project/ml-1m/ratings.dat",
	"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/Movie_Lens_Project/Output_ML1_1/",
	"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/Movie_Lens_Project/Output_ML1_2/"};
	 
	/* delete the output directory before running the job */
	FileUtils.deleteDirectory(new File(args[2]));
	FileUtils.deleteDirectory(new File(args[3]));
	 
	if (args.length != 4) {
	System.err.println("Please specify the input and output path");
	System.exit(-1);
	}
	
	System.setProperty("hadoop.home.dir","/home/hadoop/work/hadoop-3.1.2");
	
	Configuration conf = ConfigurationFactory.getInstance();
	Job sampleJob = Job.getInstance(conf);
	sampleJob.setJarByClass(ML1Driver.class);
	MultipleInputs.addInputPath(sampleJob, new Path(args[0]), TextInputFormat.class, MoviesDataMapper.class);
	MultipleInputs.addInputPath(sampleJob, new Path(args[1]), TextInputFormat.class, RatingDataMapper.class);
	sampleJob.setReducerClass(MoviesRatingJoinReducer1.class);
	sampleJob.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
	TextOutputFormat.setOutputPath(sampleJob, new Path(args[2]));
	sampleJob.setOutputKeyClass(Text.class);
	sampleJob.setOutputValueClass(Text.class);
	
	
	int code = sampleJob.waitForCompletion(true) ? 0 : 1;
	if (code == 0) {
	Job job = Job.getInstance(conf);
	job.setJarByClass(ML1Driver.class);
	job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
	job.setJobName("Highest_Rated_Movies");
	FileInputFormat.addInputPath(job, new Path(args[2]));
	FileOutputFormat.setOutputPath(job, new Path(args[3]));
	job.setMapperClass(HighestRatedMoviesMapper.class);
	job.setReducerClass(HighestRatedMoviesReducer.class);
	job.setNumReduceTasks(1);
	job.setOutputKeyClass(NullWritable.class);
	job.setOutputValueClass(Text.class);
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	}
	}
 
