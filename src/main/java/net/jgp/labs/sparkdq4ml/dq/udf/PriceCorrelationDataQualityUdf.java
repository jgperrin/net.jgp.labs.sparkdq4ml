package net.jgp.labs.sparkdq4ml.dq.udf;

import org.apache.spark.sql.api.java.UDF2;

import net.jgp.labs.sparkdq4ml.dq.service.PriceCorrelationDataQualityService;

public class PriceCorrelationDataQualityUdf implements UDF2<Integer, Integer, Integer> {

	private static final long serialVersionUID = 4949954702581973224L;

	public Integer call(Integer t1, Integer t2) throws Exception {
		return PriceCorrelationDataQualityService.checkPriceRange(t1, t2);
	}

}
