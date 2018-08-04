package sample;

import java.util.NoSuchElementException;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class SampleSparkMain {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("SampleSparkMain").setMaster("local[*]");

		try {
			conf.get("spark.master");
		} catch(NoSuchElementException e) {
			conf.setMaster("local[*]");
		}
		try (SparkSession spark = SparkSession.builder().config(conf).getOrCreate();) {
			String path = Thread.currentThread().getContextClassLoader().getResource("README.md").getPath();
			Dataset<String> logData = spark.read().textFile(path).cache();

			long numAs = logData.filter(s -> s.contains("a")).count();
			long numBs = logData.filter(s -> s.contains("b")).count();

			System.out.println("lins with a : " + numAs);
			System.out.println("lins with b : " + numBs);

			spark.stop();
		}

	}
}
