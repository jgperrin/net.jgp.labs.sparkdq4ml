package net.jgp.labs.sparkdq4ml;

import static org.apache.spark.sql.functions.callUDF;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import net.jgp.labs.sparkdq4ml.dq.udf.PriceCorrelationDataQualityUdf;
import net.jgp.labs.sparkdq4ml.dq.udf.MinimumPriceDataQualityUdf;

public class DataQuality4MachineLearningApp {

	public static void main(String[] args) {
		DataQuality4MachineLearningApp app = new DataQuality4MachineLearningApp();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("DQ4ML").master("local").getOrCreate();

		spark.udf().register("minimumPriceRule", new MinimumPriceDataQualityUdf(), DataTypes.DoubleType);
		spark.udf().register("priceCorrelationRule", new PriceCorrelationDataQualityUdf(), DataTypes.DoubleType);

		// Load our dataset
		String filename = "data/dataset.csv";
		Dataset<Row> df = spark.read().format("csv").option("inferSchema", "true").option("header", "false")
				.load(filename);
		
		// simple renaming of the columns
		df = df.withColumn("guest", df.col("_c0")).drop("_c0");
		df = df.withColumn("price", df.col("_c1")).drop("_c1");
		
		// apply DQ rules
		df = df.withColumn("price_no_min", callUDF("minimumPriceRule", df.col("price")));
		//df = df.selectExpr(exprs);
		try {
			df.createGlobalTempView("price");
		} catch (AnalysisException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		df = spark.sql("SELECT * FROM global_temp.price WHERE price_no_min > 0");
//		df = df.withColumn("price_correct_correl", callUDF("priceCorrelationRule", df.col("price_no_min")));
		
		
		df.show(50);
	}
}
