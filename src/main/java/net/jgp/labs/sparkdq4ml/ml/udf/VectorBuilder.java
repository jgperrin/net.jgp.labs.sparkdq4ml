package net.jgp.labs.sparkdq4ml.ml.udf;

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.api.java.UDF1;

public class VectorBuilder implements UDF1<Integer, Vector> {
	private static final long serialVersionUID = -2991355883253063841L;

	public Vector call(Integer t1) throws Exception {
		return Vectors.dense(t1);
	}

}
