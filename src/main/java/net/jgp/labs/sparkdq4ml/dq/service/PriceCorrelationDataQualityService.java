package net.jgp.labs.sparkdq4ml.dq.service;

public abstract class PriceCorrelationDataQualityService {

	public static double checkPriceRange(double price, int guest) {
		if (guest < 14 && price > 90) {
			return -1;
		}
		return price;
	}

}
