package test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class ReadSeq {

	public static void main(String[] args) throws Exception {
		//readTwitterSeq();
		//System.out.println("-------------------------");
		readTwitterTdidfVector();
	}
	
	public static void readTwitterSeq() throws Exception
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(
				fs,
				new Path(
						"/Users/vincent/Downloads/tdunning-MiA-5b8956f/twitter_seqfiles/part-r-00000"),
				conf);
		Text key = new Text();
		Text value = new Text();
		int l = 0;
		while (reader.next(key, value) && l < 50) {
			System.out.println(key.toString());
			System.out.println(value.toString());
			l++;
		}
		reader.close();
	}

	public static void readTwitterTdidfVector() throws Exception
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(
				fs,
				new Path(
						"/Users/vincent/Downloads/tdunning-MiA-5b8956f/twitter_vectors/tfidf-vectors/part-r-00000"),
				conf);
		Text key = new Text();
		VectorWritable value = new VectorWritable();
		int l = 0;
		while (reader.next(key, value) && l < 50) {
			System.out.println(key.toString());
			System.out.println(value.toString());
			l++;
		}
		reader.close();
	}
}
