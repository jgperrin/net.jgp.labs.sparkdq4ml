package net.jgp.labs.sparkdq4ml.dq;

import org.apache.spark.sql.api.java.UDF1;

public class Multiplier2 implements UDF1<Integer, Integer> {

	private static final long serialVersionUID = -4519338105113996424L;

	public Integer call(Integer t1) throws Exception {
		return t1 * 2;
	}

}
