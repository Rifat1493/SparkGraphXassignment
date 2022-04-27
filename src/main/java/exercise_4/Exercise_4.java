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

		String vertice_path = "C:\\Users\\Rifat\\Desktop\\SparkGraphXassignment\\src\\main\\resources\\wiki-vertices.txt";
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

		//gf.vertices().show(1);
		//gf.edges().show(1);
		//double damping_factor = .85;

		//Dataset<Row> result = gf.pageRank().maxIter(1).resetProbability(1-damping_factor).run().vertices();
		//gf.pageRank().resetProbability(0.15).run().vertices().show(10);
		//result.orderBy(desc("pagerank")).show();



       // Identifying the best parameter value
		Dataset<Row> best_model = spark.emptyDataFrame();;
		double damp_fact_list[] = {.75,.85,.90}, prev_value = 0,prev = 1, bst_dmp = 0, bst_prev_percent, min_percent = 0,tmp;
		int i, j, bst_it = 0;


		// iterating over an array

		for (j = 0; j < damp_fact_list.length; j++) {
			for (i = 8; i < 15; i++) {

				if (i == 8 & j ==0){
					Dataset<Row> result = gf.pageRank().maxIter(i).resetProbability(1-damp_fact_list[j]).run().vertices();
					//gf.pageRank().resetProbability(0.15).run().vertices().show(10);
					prev_value = result.orderBy(desc("pagerank")).limit(10).agg(sum("pagerank")).first().getDouble(0);
                    min_percent = 100;
					//System.out.println(prev_value);
					best_model = result;
				}
				else {
					Dataset<Row> result = gf.pageRank().maxIter(i).resetProbability(1 - damp_fact_list[j]).run().vertices();
					//gf.pageRank().resetProbability(0.15).run().vertices().show(10);
					Double sum_value = result.orderBy(desc("pagerank")).limit(10).agg(sum("pagerank")).first().getDouble(0);

					tmp = Math.abs((sum_value - prev_value)) / prev_value;
					prev_value = sum_value;
					System.out.println("percent change with iteration"+i+" damp factor "+damp_fact_list[j] + "value"+tmp);
					if (tmp < min_percent) {
						min_percent = tmp;
						bst_it = i;
						bst_dmp= damp_fact_list[j];
						best_model = result;
					}
				}
			}
		}


         System.out.println("best iteration = "+bst_it+"\t best damping factor = "+bst_dmp);


         best_model.orderBy(desc("pagerank")).show(10);




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


		 Double result = vertices.orderBy(desc("name")).limit(2).agg(sum("name")).first().getDouble(0);

		 System.out.println(result);

		double damp_fact_list[] = {.75,.85,.90};
		int i, j;

        float prev =1, bst_dmp, bst_prev_percent, min_percent;
		int bst_it;
		// iterating over an array

			for (j = 0; j < damp_fact_list.length; j++) {
				for (i = 8; i < 15; i++) {

				float x = 10;
				float tmp = Math.abs((x-prev))/prev;
				//System.out.println(tmp);

				}

			}

	}

	}
	

