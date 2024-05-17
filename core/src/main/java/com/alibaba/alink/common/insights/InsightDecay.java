package com.alibaba.alink.common.insights;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class InsightDecay {
	private HashMap <ConstraintType, Integer> constraintMap = new HashMap <>();
	private HashMap <String, Integer> keyCount = new HashMap <>();

	public enum ConstraintType {
		Breakdown,
		Insight,
		MeasureCol,
		MeasureColAndType,
		SubspaceCol,
		SubspaceColAndValue,
		BreakdownMeasureColAndType,
		BreakdownMeasureCol,
		InsightBreakdownMeasureColAndType,
		InsightBreakdownMeasureCol,
		InsightBreakdown,
	}

	public InsightDecay() {
		constraintMap.put(ConstraintType.Breakdown, 3);
		constraintMap.put(ConstraintType.Insight, 1);
		constraintMap.put(ConstraintType.MeasureCol, 3);
		constraintMap.put(ConstraintType.MeasureColAndType, 2);
		constraintMap.put(ConstraintType.SubspaceCol, 3);
		constraintMap.put(ConstraintType.SubspaceColAndValue, 1);
		constraintMap.put(ConstraintType.BreakdownMeasureColAndType, 1);
		constraintMap.put(ConstraintType.BreakdownMeasureCol, 2);
		constraintMap.put(ConstraintType.InsightBreakdownMeasureColAndType, 1);
		constraintMap.put(ConstraintType.InsightBreakdownMeasureCol, 2);
		constraintMap.put(ConstraintType.InsightBreakdown, 2);
	}

	private String getKey(Insight insight, ConstraintType type) {
		List <Subspace> subspaces = new ArrayList <>();
		Tuple2 <String, String> value;
		switch (type) {
			case Insight:
				return "insight_" + insight.type.toString();
			case Breakdown:
				if (null == insight.subject || null == insight.subject.breakdown) {
					return null;
				}
				return "breakdown_" + insight.subject.breakdown.colName;
			case MeasureCol:
				if (null == insight.subject || null == insight.subject.measures
					|| insight.subject.measures.size() == 0) {
					return null;
				}
				return getMeasureCol(insight.subject.measures);
			case MeasureColAndType:
				if (null == insight.subject || null == insight.subject.measures
					|| insight.subject.measures.size() == 0) {
					return null;
				}
				return getMeasureColAndType(insight.subject.measures);
			case SubspaceCol:
				if (null == insight.subject || null == insight.subject.subspaces
					|| insight.subject.subspaces.size() == 0) {
					return null;
				}
				subspaces.addAll(insight.subject.subspaces);
				if (insight.attachSubspaces.size() > 0) {
					subspaces.addAll(insight.attachSubspaces);
				}
				return getSubspaceCol(subspaces);
			case SubspaceColAndValue:
				if (null == insight.subject || null == insight.subject.subspaces
					|| insight.subject.subspaces.size() == 0) {
					return null;
				}
				subspaces.addAll(insight.subject.subspaces);
				if (insight.attachSubspaces.size() > 0) {
					subspaces.addAll(insight.attachSubspaces);
				}
				return getSubspaceColAndValue(subspaces);
			case InsightBreakdown:
				if (null == insight.subject || null == insight.subject.breakdown) {
					return null;
				}
				return "insight_" + insight.type.toString() + ";breakdown_" + insight.subject.breakdown.colName;
			case InsightBreakdownMeasureCol:
				if (null == insight.subject || null == insight.subject.breakdown ||
					null == insight.subject.measures || insight.subject.measures.size() == 0) {
					return null;
				}
				value = getMeasureKey(insight.subject.measures);
				return "insight_" + insight.type.toString() + ";breakdown_" + insight.subject.breakdown.colName + ";"
					+ value.f0;
			case InsightBreakdownMeasureColAndType:
				if (null == insight.subject || null == insight.subject.breakdown ||
					null == insight.subject.measures || insight.subject.measures.size() == 0) {
					return null;
				}
				value = getMeasureKey(insight.subject.measures);
				return "insight_" + insight.type.toString() + ";breakdown_" + insight.subject.breakdown.colName + ";"
					+ value.f1;
			case BreakdownMeasureColAndType:
				if (null == insight.subject || null == insight.subject.breakdown ||
					null == insight.subject.measures || insight.subject.measures.size() == 0) {
					return null;
				}
				value = getMeasureKey(insight.subject.measures);
				return "breakdown_" + insight.subject.breakdown.colName + ";" + value.f1;
			case BreakdownMeasureCol:
				if (null == insight.subject || null == insight.subject.breakdown ||
					null == insight.subject.measures || insight.subject.measures.size() == 0) {
					return null;
				}
				value = getMeasureKey(insight.subject.measures);
				return "breakdown_" + insight.subject.breakdown.colName + ";" + value.f0;
			default:
				return null;
		}
	}

	private Tuple2 <String, String> getMeasureKey(List <Measure> measures) {
		StringBuilder measureBuilder = new StringBuilder();
		StringBuilder measureTypeBuilder = new StringBuilder();
		for (int i = 0; i < measures.size(); i++) {
			measureBuilder.append("measure_");
			measureBuilder.append(measures.get(i).colName);
			measureTypeBuilder.append("measure_").append(measures.get(i).colName);
			measureTypeBuilder.append("_type_").append(measures.get(i).aggr.toString());
			if (i < measures.size() - 1) {
				measureBuilder.append(";");
				measureTypeBuilder.append(";");
			}
		}
		return Tuple2.of(measureBuilder.toString(), measureTypeBuilder.toString());
	}

	public InsightDecay addConstraint(ConstraintType type, int num) {
		constraintMap.put(type, num);
		return this;
	}

	public InsightDecay removeConstraint(ConstraintType type) {
		if (constraintMap.containsKey(type)) {
			constraintMap.remove(type);
		}
		return this;
	}

	private String getMeasureCol(List <Measure> measures) {
		StringBuilder measureBuilder = new StringBuilder();
		measureBuilder.append("measure_");
		measureBuilder.append(measures.get(0).colName);
		if (measures.size() == 2) {
			measureBuilder.append(";").append("measure_").append(measures.get(1).colName);
		}
		return measureBuilder.toString();
	}

	private String getMeasureColAndType(List <Measure> measures) {
		StringBuilder measureBuilder = new StringBuilder();
		measureBuilder.append("measure_");
		measureBuilder.append(measures.get(0).colName);
		measureBuilder.append("_aggr_").append(measures.get(0).aggr.toString());
		if (measures.size() == 2) {
			measureBuilder.append("measure_");
			measureBuilder.append(measures.get(1).colName);
			measureBuilder.append("_aggr_").append(measures.get(1).aggr.toString());
		}
		return measureBuilder.toString();
	}

	private String getSubspaceCol(List <Subspace> subspaces) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < subspaces.size(); i++) {
			builder.append("subspace_").append(subspaces.get(i).colName);
			if (i != subspaces.size() - 1) {
				builder.append(";");
			}
		}
		return builder.toString();
	}

	private String getSubspaceColAndValue(List <Subspace> subspaces) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < subspaces.size(); i++) {
			builder.append("subspace_").append(subspaces.get(i).colName);
			builder.append("_value_").append(subspaces.get(i).value.toString());
			if (i != subspaces.size() - 1) {
				builder.append(";");
			}
		}
		return builder.toString();
	}

	private double getInsightDecay(Insight insight) {
		// 记录原始分数
		insight.originScore = insight.score;
		double overCount = 0;
		for (Entry <ConstraintType, Integer> constraintEntry : this.constraintMap.entrySet()) {
			String keyName = getKey(insight, constraintEntry.getKey());
			if (null == keyName || keyName.length() == 0) {
				continue;
			}
			int count = keyCount.getOrDefault(keyName, 0);
			keyCount.put(keyName, ++count);
			if (count > constraintEntry.getValue()) {
				overCount = Math.max(overCount, count - constraintEntry.getValue());
				//overCount ++;
			}
		}
		overCount = Math.min(
			keyCount.get(getKey(insight, ConstraintType.Insight)) - constraintMap.get(ConstraintType.Insight),
			overCount);
		if (overCount == 0) {
			return 1.0;
		} else {
			return 1.0 / (1.0 + Math.log(overCount + 1) * 0.1);
		}
		//return 1.0;
	}

	public static List <Insight> sortTopInsights(List <Insight> insights, double highScore, double lowScore) {
		Map <InsightType, List <Insight>> topInsightsMap = new HashMap <>();

		Comparator <Insight> comaprator = new Comparator <Insight>() {
			@Override
			public int compare(Insight o1, Insight o2) {
				return -Double.compare(o1.score, o2.score);
			}
		};
		double mapTotalInsightNum = 0;
		double maxInsightNum = 0;
		for (Insight insight : insights) {
			if (insight.score >= lowScore && insight.score < highScore) {
				if (!topInsightsMap.containsKey(insight.type)) {
					topInsightsMap.put(insight.type, new ArrayList <>());
				}
				topInsightsMap.get(insight.type).add(insight);
				mapTotalInsightNum ++;
				maxInsightNum = Math.max(maxInsightNum, topInsightsMap.get(insight.type).size());
			}
		}
		List <Insight> topList = new ArrayList <>();
		// insight type less than 3 or 60% has same insight type
		if (topInsightsMap.isEmpty() || topInsightsMap.size() <= 3 || maxInsightNum / mapTotalInsightNum > 0.6) {
			return topList;
		}
		while (true) {
			List <Insight> tmpList = new ArrayList <>();
			HashSet<String> xyAxisSet = new HashSet<>();
			for (Entry <InsightType, List <Insight>> entry : topInsightsMap.entrySet()) {
				if (!entry.getValue().isEmpty()) {
					for (int i = 0; i < entry.getValue().size(); i++) {
						Insight insight = entry.getValue().get(i);
						if (null == insight.layout) {
							continue;
						}
						String xAxis = insight.layout.xAxis;
						String yAxis = insight.layout.yAxis;
						if (xAxis == null) {
							xAxis = "";
						}
						if (yAxis == null) {
							yAxis = "";
						}
						String xy = xAxis + yAxis;
						if (xy.length() == 0) {
							continue;
						}
						String yx = yAxis + xAxis;
						// if only 1 insight, just add
						if (xyAxisSet.contains(xy) || xyAxisSet.contains(yx) && entry.getValue().size() > 1) {
							// last one, add first one
							if (i == entry.getValue().size() - 1) {
								tmpList.add(entry.getValue().get(0));
								entry.getValue().remove(0);
								break;
							}
							continue;
						}
						tmpList.add(insight);
						entry.getValue().remove(i);
						xyAxisSet.add(xy);
						xyAxisSet.add(yx);
						break;
					}
				}
			}
			if (tmpList.isEmpty()) {
				break;
			}
			tmpList.sort(comaprator);
			topList.addAll(tmpList);
		}
		return topList;
	}

	public static List <Insight> sortInsights(List <Insight> insights, double lowScore) {
		insights.sort(new Comparator <Insight>() {
			@Override
			public int compare(Insight o1, Insight o2) {
				return -Double.compare(o1.score, o2.score);
			}
		});
		double[] thresholds = new double[]{1.0, 0.8, 0.5, 0.1, 0.01};
		double highScore = thresholds[0];
		List <Insight> sortResult = new ArrayList <>();
		for (int i = 1; i < thresholds.length; i++) {
			List <Insight> tmpList = InsightDecay.sortTopInsights(insights, highScore, thresholds[i]);
			if (tmpList.size() > 0) {
				sortResult.addAll(tmpList);
				highScore = thresholds[i];
			}
		}
		if (sortResult.size() == 0) {
			return insights;
		}
		return sortResult;
	}

	public static List <Insight> sortInsights(List <Insight> insights) {
		return sortInsights(insights, 0.5);
	}

}
