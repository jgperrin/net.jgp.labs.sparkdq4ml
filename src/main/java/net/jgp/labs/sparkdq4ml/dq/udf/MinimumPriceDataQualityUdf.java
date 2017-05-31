package net.jgp.labs.sparkdq4ml.dq.udf;

import org.apache.spark.sql.api.java.UDF1;

import net.jgp.labs.sparkdq4ml.dq.service.MinimumPriceDataQualityService;

public class MinimumPriceDataQualityUdf implements UDF1<Double, Double> {

	private static final long serialVersionUID = -201966159201746851L;

	public Double call(Double t1) throws Exception {
		return MinimumPriceDataQualityService.checkMinimumPrice(t1);
	}
}
