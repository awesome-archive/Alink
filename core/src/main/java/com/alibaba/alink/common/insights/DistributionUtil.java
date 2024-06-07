package com.alibaba.alink.common.insights;

import org.apache.commons.math3.distribution.ChiSquaredDistribution;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.distribution.LogisticDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DistributionUtil {

	public enum DistributionType implements Serializable {
		Normal("正态分布", "Normal Distribution"),
		Uniform("均匀分布", "Uniform Distribution"),
		ChiSquare("卡方分布", "ChiSquare Distribution"),
		Exp("指数分布", "Exp Distribution"),
		T("T分布", "T Distribution"),
		Logistic("Logistic分布", "Logistic Distribution"),
		LogNormal("对数正态分布", "LogNormal Distribution");

		private final String cnName;
		private final String enName;

		private DistributionType(String cnName, String enName) {
			this.cnName = cnName;
			this.enName = enName;
		}

		public String getCnName() {
			return this.cnName;
		}

		public String getEnName() {
			return this.enName;
		}

	}

	public static class DistributionInfo implements Serializable {
		DistributionType type;
		double score;
		public DistributionInfo(DistributionType type, double score) {
			this.type = type;
			this.score = score;
		}

		public String getCnDesc() {
			return String.format("符合%s", type.getCnName());
		}

		public String getEnDesc() {
			return String.format("data flows %s", type.getEnName());
		}
	}

	public static final KolmogorovSmirnovTest KS_TEST = new KolmogorovSmirnovTest();

	public static DistributionInfo testDistribution(List <Number> dataList) {
		double[] datas = loadDataToVec(dataList);
		if (datas.length == 0) {
			return null;
		}
		double maxScore = 0;
		List<DistributionInfo> list = new ArrayList <>();
		try {
			list.add(testNormalDistribution(datas));
			list.add(testUniformDistribution(datas));
			list.add(testChiSquaredDistribution(datas));
			list.add(testExpDistribution(datas));
			list.add(testTDistribution(datas));
			list.add(testLogisticDistribution(datas));
			list.add(testLogNormalDistribution(datas));
		} catch (Exception e) {
			System.out.println(e);
		}
		int maxIndex = 0;
		for (int i = 0; i < list.size(); i++) {
			if (maxScore < list.get(i).score) {
				maxIndex = i;
				maxScore = list.get(i).score;
			}
		}
		if (maxScore > 0.05) {
			return list.get(maxIndex);
		} else {
			return null;
		}
	}

	public static double[] loadDataToVec(List <Number> dataList) {
		double[] datas = new double[dataList.size()];
		for (int i = 0; i < dataList.size(); i++) {
			datas[i] = Double.valueOf(String.valueOf(dataList.get(i)));
		}
		return datas;
	}

	public static double getSd(double[] datas) {
		double avg = Arrays.stream(datas).average().getAsDouble();
		double variance = 0;
		for (int i = 0; i < datas.length; i++) {
			variance += Math.pow(datas[i] - avg, 2);
		}
		variance = variance / datas.length;
		double sd = Math.sqrt(variance);
		return sd;
	}

	public static DistributionInfo testNormalDistribution(double[] datas) {
		double sd = getSd(datas);
		if (sd == 0) {
			return new DistributionInfo(DistributionType.Normal, 0);
		}
		double avg = Arrays.stream(datas).average().getAsDouble();
		NormalDistribution distribution = new NormalDistribution(avg, sd);
		return new DistributionInfo(DistributionType.Normal, KS_TEST.kolmogorovSmirnovTest(distribution, datas));
	}

	public static DistributionInfo testUniformDistribution(double[] datas) {
		double lower = Arrays.stream(datas).min().getAsDouble();
		double upper = Arrays.stream(datas).max().getAsDouble();
		if (upper - lower <= Math.abs(lower) * 0.0001) {
			return new DistributionInfo(DistributionType.Uniform, 0);
		}

		UniformRealDistribution distribution = new UniformRealDistribution(lower, upper);
		return new DistributionInfo(DistributionType.Uniform, KS_TEST.kolmogorovSmirnovTest(distribution, datas));
	}

	// if positive, return degreeOfFreedom
	public static DistributionInfo testChiSquaredDistribution(double[] datas) {
		for (int i = 1; i <= 20; i++) {
			ChiSquaredDistribution distribution = new ChiSquaredDistribution(i);
			double p = KS_TEST.kolmogorovSmirnovTest(distribution, datas);
			if (p > 0.05 ) {
				return new DistributionInfo(DistributionType.ChiSquare, KS_TEST.kolmogorovSmirnovTest(distribution, datas));
			}
		}
		return new DistributionInfo(DistributionType.ChiSquare, 0);
	}

	public static DistributionInfo testExpDistribution(double[] datas) {
		double avg = Arrays.stream(datas).average().getAsDouble();
		if (avg <= 0) {
			return new DistributionInfo(DistributionType.Exp, 0);
		}
		ExponentialDistribution distribution = new ExponentialDistribution(avg);
		return new DistributionInfo(DistributionType.Exp, KS_TEST.kolmogorovSmirnovTest(distribution, datas));
	}

	// if positive, return degreeOfFreedom
	public static DistributionInfo testTDistribution(double[] datas) {
		for (int i = 1; i <= 20; i++) {
			TDistribution distribution = new TDistribution(i);
			double p = KS_TEST.kolmogorovSmirnovTest(distribution, datas);
			if (p > 0.05) {
				return new DistributionInfo(DistributionType.T, KS_TEST.kolmogorovSmirnovTest(distribution, datas));
			}
		}
		return new DistributionInfo(DistributionType.T, 0);
	}

	public static DistributionInfo testLogisticDistribution(double[] datas) {
		double avg = Arrays.stream(datas).average().getAsDouble();
		double sd = getSd(datas);
		if (sd == 0) {
			return new DistributionInfo(DistributionType.Logistic, 0);
		}
		LogisticDistribution distribution = new LogisticDistribution(avg, sd);
		return new DistributionInfo(DistributionType.Logistic, KS_TEST.kolmogorovSmirnovTest(distribution, datas));
	}

	public static DistributionInfo testLogNormalDistribution(double[] datas) {
		double min = Arrays.stream(datas).min().getAsDouble();
		if (min <= 0) {
			return new DistributionInfo(DistributionType.LogNormal, 0);
		}
		double[] logValues = new double[datas.length];
		for (int i = 0; i < datas.length; i++) {
			logValues[i] = Math.log(datas[i]);
		}
		double avg = Arrays.stream(logValues).average().getAsDouble();
		double var = 0;
		for (int i = 0; i < logValues.length; i++) {
			var += Math.pow((logValues[i] - avg), 2);
		}
		var = Math.sqrt(var);
		LogNormalDistribution distribution = new LogNormalDistribution(avg, var);
		return new DistributionInfo(DistributionType.LogNormal, KS_TEST.kolmogorovSmirnovTest(distribution, datas));
	}


}
