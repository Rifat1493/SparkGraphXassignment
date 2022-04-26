package exercise_4;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;

public class Exercise_4 {
	

	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) throws Exception {
		SparkSession spark = SparkSession.builder()
				.master("local[*]")
				.appName("SparkByExamples.com")
				.getOrCreate();

		String vertice_path = "C:\\Users\\Rifat\\Desktop\\SparkGraphXassignment\\src\\main\\resources\\wiki-vertices-test.txt";
		String edge_path = "C:\\Users\\Rifat\\Desktop\\SparkGraphXassignment\\src\\main\\resources\\wiki-edges.txt";
		Dataset<Row> df1 = spark.read().text(vertice_path);
		Dataset<Row> df2 = spark.read().text(edge_path);
		Dataset<Row> vertices = spark.emptyDataFrame();
		Dataset<Row> edges = spark.emptyDataFrame();

		//df1.show(1);


		vertices = df1.withColumn("id", split(col("value"), "\\t").getItem(0))
				.withColumn("name", split(col("value"), "\\t").getItem(1)).drop(col("value"));

		edges = df2.withColumn("src", split(col("value"), "\\t").getItem(0))
				.withColumn("dst", split(col("value"), "\\t").getItem(1)).drop(col("value"));



		GraphFrame gf = GraphFrame.apply(vertices,edges);

		gf.vertices().show(1);
		gf.edges().show(1);
		double damping_factor = .85;

		Dataset<Row> result = gf.pageRank().maxIter(1).resetProbability(1-damping_factor).run().vertices();
		//gf.pageRank().resetProbability(0.15).run().vertices().show(10);
		result.orderBy(desc("pagerank")).show();



	}

	public static void wikipedia1(JavaSparkContext ctx, SQLContext sqlCtx) throws Exception {
		SparkSession spark = SparkSession.builder()
				.master("local[*]")
				.appName("SparkByExamples.com")
				.getOrCreate();

		String vertice_path = "C:\\Users\\Rifat\\Desktop\\SparkGraphXassignment\\src\\main\\resources\\wiki-vertices-test.txt";
		Dataset<Row> df1 = spark.read().text(vertice_path);
		Dataset<Row> vertices = spark.emptyDataFrame();
		vertices = df1.withColumn("id", split(col("value"), "\\t").getItem(0))
				.withColumn("name", split(col("value"), "\\t").getItem(1)).drop(col("value"));

		df1.show();
		//Dataset<Row> df2 = df1.orderBy(desc(""))



	}



	}
	

