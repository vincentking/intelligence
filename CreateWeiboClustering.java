package mia.clustering.ch12;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class CreateWeiboClustering {

	private static List<Vector> vectors = new ArrayList<Vector>();

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		//readVectors(conf, fs);
		//writeCluster(conf, fs, vectors);
		runKMeans(conf);
	}

	public static void readVectors(Configuration conf, FileSystem fs)
			throws IOException {
		SequenceFile.Reader reader = new SequenceFile.Reader(
				fs,
				new Path(
						"/Users/vincent/Downloads/tdunning-MiA-5b8956f/twitter_vectors/tfidf-vectors/part-r-00000"),
				conf);
		Text key = new Text();
		VectorWritable value = new VectorWritable();
		int l = 0;

		while (reader.next(key, value) && l < 100) {
			System.out.println(key.toString());
			System.out.println(value.toString());
			l++;
			vectors.add(value.get());
		}
		reader.close();
	}

	public static void writeCluster(Configuration conf, FileSystem fs,
			List<Vector> vectors) throws IOException {
		SequenceFile.Writer writer = new SequenceFile.Writer(
				fs,
				conf,
				new Path(
						"/Users/vincent/Downloads/tdunning-MiA-5b8956f/twitter_clusters/part-00000"),
				Text.class, Cluster.class);
		for (int i = 0; i < vectors.size(); i++) {
			Vector vec = vectors.get(i);
			Cluster cluster = new Cluster(vec, i,
					new EuclideanDistanceMeasure());
			System.out.println("****" + cluster.getIdentifier());
			writer.append(new Text(cluster.getIdentifier()), cluster);
		}
		writer.close();
	}

	public static void runKMeans(Configuration conf) throws IOException,
			InterruptedException, ClassNotFoundException {
		KMeansDriver
				.run(conf,
						new Path(
								"/Users/vincent/Downloads/tdunning-MiA-5b8956f/twitter_vectors/tfidf-vectors/"),
						new Path(
								"/Users/vincent/Downloads/tdunning-MiA-5b8956f/twitter_clusters/"),
						new Path(
								"/Users/vincent/Downloads/tdunning-MiA-5b8956f/twitter_cluster_output/"),
						new EuclideanDistanceMeasure(), 0.001, 100, true, false);
	}
	
	public static void readClusters(Configuration conf, FileSystem fs) throws IOException
	{
		SequenceFile.Reader reader = new SequenceFile.Reader(fs,
		        new Path(""), conf);
		    Text key = new Text();
		    WeightedVectorWritable value = new WeightedVectorWritable();
		    while (reader.next(key, value)) {
		      System.out.println(value.toString() + " belongs to cluster "
		                         + key.toString());
		    }
		    reader.close();
	}
}
