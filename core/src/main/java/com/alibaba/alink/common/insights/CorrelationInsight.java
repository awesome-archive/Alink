package com.alibaba.alink.common.insights;

import org.apache.flink.api.java.tuple.Tuple3;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.local.LocalOperator;

import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;
import org.apache.commons.math3.stat.correlation.SpearmansCorrelation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class CorrelationInsight extends CorrelationInsightBase {

	public CorrelationInsight(Insight insight) {
		super(insight);
	}

	@Override
	public Insight processData(LocalOperator <?>... sources) {
		LocalOperator <?>[] sourceInput = new LocalOperator[]{sources[0], sources[1]};
		if (needFilter) {
			sourceInput[0] = Mining.filter(sourceInput[0], this.insight.subject.subspaces);
			sourceInput[1] = Mining.filter(sourceInput[1], this.insight.attachSubspaces);
		}
		if (needGroup) {
			sourceInput[0] = groupData(sourceInput[0], insight.subject).get(0);
			sourceInput[1] = groupData(sourceInput[1], insight.subject).get(0);
		}
		insight.score = computeScore(sourceInput);
		return this.insight;
	}

	public void fillLayout(double score) {
		String correlation = "正相关";
		if (score < 0) {
			correlation = "负相关";
		}
		List <Measure> measures = this.insight.subject.measures;
		this.insight.layout.xAxis = this.insight.subject.breakdown.colName;
		this.insight.layout.xAlias = this.insight.subject.breakdown.getColCnName();
		this.insight.layout.yAxis = measures.get(0).aggr + "(" + measures.get(0).colName + ")";
		this.insight.layout.yAlias = "%s的%s".format(measures.get(0).getColCnName(), measures.get(0).aggr.getCnName());
		this.insight.layout.lineA = insight.getSubspaceStr(insight.subject.subspaces);
		this.insight.layout.lineB = insight.getSubspaceStr(insight.attachSubspaces);
		this.insight.layout.title = "子集的统计指标" +
			String.format("%s的%s", measures.get(0).getColCnName(), measures.get(0).aggr.getCnName())
			+ "存在" + correlation;
		StringBuilder builder = new StringBuilder();
		builder.append(insight.layout.lineA).append("与").append(insight.layout.lineB).append("条件下，");
		builder.append("统计指标")
			.append(String.format("%s的%s", measures.get(0).getColCnName(), measures.get(0).aggr.getCnName()))
			.append("存在")
			.append(correlation);
		//if (this.range.intValue() > MAX_SCALAR_THRESHOLD.intValue()) {
		//	builder.append("*由于二者数值范围差异较大，对第二条线进行了缩放");
		//}
		this.insight.layout.description = builder.toString();
	}

	public double computeScore(LocalOperator <?>... sources) {
		//String[] columns = new String[] {insight.subject.breakdown.colName, MEASURE_NAME_PREFIX + "0"};
		HashMap <Object, Number> meaValues1 = initData(sources[0]);
		HashMap <Object, Number> meaValues2 = initData(sources[1]);
		List <Tuple3 <Number, Number, Object>> points = new ArrayList <>();
		for (Entry <Object, Number> entry : meaValues1.entrySet()) {
			if (!meaValues2.containsKey(entry.getKey())) {
				continue;
			}
			points.add(Tuple3.of(entry.getValue(), meaValues2.get(entry.getKey()), entry.getKey()));
		}
		if (points.size() < MIN_SAMPLE_NUM) {
			return 0;
		}
		double[] xArray = new double[points.size()];
		double[] yArray = new double[points.size()];
		double maxX = Double.MIN_VALUE;
		double maxY = Double.MIN_VALUE;
		double minX = Double.MAX_VALUE;
		double minY = Double.MAX_VALUE;
		for (int i = 0; i < points.size(); i++) {
			xArray[i] = points.get(i).f0.doubleValue();
			yArray[i] = points.get(i).f1.doubleValue();
			maxX = Math.max(maxX, xArray[i]);
			maxY = Math.max(maxY, yArray[i]);
			minX = Math.min(minX, xArray[i]);
			minY = Math.min(minY, yArray[i]);
		}
		if (maxX - minX == 0 || maxY - minY == 0) {
			return 0;
		}

		WeightedObservedPoints weightedObservedPoints = new WeightedObservedPoints();
		for (int i = 0; i < points.size(); i++) {
			weightedObservedPoints.add(xArray[i], yArray[i]);
		}
		PolynomialCurveFitter polynomialCurveFitter = PolynomialCurveFitter.create(1);
		double[] params = polynomialCurveFitter.fit(weightedObservedPoints.toList());
		double r2 = 0.0;
		for (int i = 0; i < points.size(); i++) {
			r2 += Math.pow(params[0] + params[1] * xArray[i] - yArray[i], 2);
		}
		double scoreA = 1 - Math.sqrt(r2) / ((maxY - minY) * points.size());
		if (scoreA < 0) {
			return 0;
		}

		//PearsonsCorrelation pc = new PearsonsCorrelation();
		SpearmansCorrelation sc = new SpearmansCorrelation();
		double score = Math.abs(sc.correlation(xArray, yArray));
		if (score >= MIN_CORRELATION_THRESHOLD) {
			MTable mtable = mergeData(points, sources[0].getSchema(), sources[1].getSchema());
			insight.layout.data = mtable;
			this.fillLayout(params[1]);
		} else {
			score = 0;
		}
		return score;
	}

}
