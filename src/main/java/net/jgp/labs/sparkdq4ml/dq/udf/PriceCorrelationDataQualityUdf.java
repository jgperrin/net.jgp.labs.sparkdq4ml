package net.jgp.labs.sparkdq4ml.dq.udf;

import org.apache.spark.sql.api.java.UDF2;

import net.jgp.labs.sparkdq4ml.dq.service.PriceCorrelationDataQualityService;

public class PriceCorrelationDataQualityUdf implements UDF2<Double, Integer, Double> {

	private static final long serialVersionUID = 4949954702581973224L;

	public Double call(Double price, Integer guest) throws Exception {
		return PriceCorrelationDataQualityService.checkPriceRange(price, guest);
	}

}
