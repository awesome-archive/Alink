package com.alibaba.alink.common.insights;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.utils.TableUtil;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class DvInsightDescription implements Serializable {

	public Meta meta;
	public Result result;
	public View view;

	public enum ColType {
		categorical,
		numerical,
		temporal
	}

	public static class Data implements Serializable {
		public String type = "static";
		public Map <String, Object>[] content;
	}

	public static class Field implements Serializable {
		String id;
		String code;

		String alias;

		ColType type;

		String[] characteristics = new String[0];

		Abstraction abstraction;
	}

	public static class Meta implements Serializable {
		public double score;
		public Field[] fields;
		public Data data;
	}

	public static class Subspace implements Serializable {
		String fieldId;
		String[] range;
	}

	public static class Breakdown implements Serializable {
		String fieldId;
	}

	public static class Measure implements Serializable {
		String fieldId;
	}

	public static class Target implements Serializable {
		public Subspace[] subspace;
		public Breakdown[] breakdowns;
		public Measure[] measures;

	}

	public static class Result implements Serializable {
		Target target;
		String text;
		String factType;
	}

	public static class Abstraction implements Serializable {
		public String aggregation;
	}

	public static class Trend implements Serializable {
		public Object[] from;
		public Object[] to;
	}

	public static class Task implements Serializable {
		public String type;
		public String[] dimensions;
		public String[] measures;
		public String[][] focuses;

		public String[] seasonality;

		public Trend trend;
	}

	public static class DatavFilter implements Serializable {
		public String type = "value";
		public String code;
		public String[] values;
	}

	public static class View implements Serializable {
		public DatavFilter[] subspaces;
		public String[] attributes;

		public Task[] tasks;
	}

	public static DvInsightDescription of(Insight insight) {
		return DvInsightDescription.of(insight, new HashMap <>());
	}

	public static DvInsightDescription of(Insight insight, Map<String, String> cnNamesMap) {
		InsightType type = insight.type;
		int colNum = insight.layout.data.getNumCol();
		int rowNum = insight.layout.data.getNumRow();
		String[] colNames = insight.layout.data.getColNames();
		TypeInformation <?>[] colTypes = insight.layout.data.getColTypes();

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		Field[] fields = new Field[colNum];
		if (type == InsightType.BasicStat) {
			String colName = insight.layout.xAxis;
			for (int i = 0; i < colNum; i++) {
				fields[i] = new Field();
				fields[i].id = colNames[i];
				fields[i].code = colNames[i];
				fields[i].alias = cnNamesMap.getOrDefault(colName, colName);
				fields[i].abstraction = new Abstraction();
				if (i == 0) {
					fields[i].abstraction.aggregation = "COUNTDISTINCT";
				} else if (i == 1) {
					fields[i].abstraction.aggregation = MeasureAggr.COUNT.getEnName();
				} else if (i == 2) {
					fields[i].abstraction.aggregation = MeasureAggr.MAX.getEnName();
				} else if (i == 3) {
					fields[i].abstraction.aggregation = MeasureAggr.MIN.getEnName();
				} else if (i == 4) {
					fields[i].abstraction.aggregation = MeasureAggr.SUM.getEnName();
				} else if (i == 5) {
					fields[i].abstraction.aggregation = MeasureAggr.AVG.getEnName();
				}
			}
		} else if (type == InsightType.Distribution) {
			for (int i = 0; i < colNum; i++) {
				fields[i] = new Field();
				fields[i].id = colNames[i];
				fields[i].code = colNames[i];
				if (i != 0) {
					String colName = insight.subject.measures.get(i - 1).colName;
					fields[i].alias = cnNamesMap.getOrDefault(colName, colName);
					fields[i].abstraction = new Abstraction();
					fields[i].abstraction.aggregation = insight.subject.measures.get(i - 1).aggr.getEnName();
				}
			}
		} else if (type == InsightType.CrossMeasureCorrelation) {
			for (int i = 0; i < colNum; i++) {
				fields[i] = new Field();
				fields[i].id = colNames[i];
				fields[i].code = colNames[i];
				if (i >= 1) {
					if (insight.subject != null && insight.subject.measures != null
						&& i - 1 < insight.subject.measures.size()) {
						String colName = insight.subject.measures.get(i - 1).colName;
						fields[i].alias = cnNamesMap.getOrDefault(colName, colName);
						fields[i].abstraction = new Abstraction();
						fields[i].abstraction.aggregation = insight.subject.measures.get(i - 1).aggr.getEnName();
					}
				}
			}
		} else if (type == InsightType.Correlation) {
			for (int i = 0; i < colNum; i++) {
				fields[i] = new Field();
				fields[i].id = colNames[i];
				fields[i].code = colNames[i];
				if (i >= 1) {
					if (insight.subject != null && insight.subject.measures != null) {
						String colName = insight.subject.measures.get(0).colName;
						fields[i].alias = cnNamesMap.getOrDefault(colName, colName);
						fields[i].abstraction = new Abstraction();
						fields[i].abstraction.aggregation = insight.subject.measures.get(0).aggr.getEnName();
					}
				}
			}
		} else if (type == InsightType.Clustering2D) {
			for (int i = 0; i < colNum; i++) {
				fields[i] = new Field();
				fields[i].id = colNames[i];
				fields[i].code = colNames[i];
				if (i >= 1) {
					if (insight.subject != null && insight.subject.measures != null
						&& i - 1 < insight.subject.measures.size()) {
						String colName = insight.subject.measures.get(i - 1).colName;
						fields[i].alias = cnNamesMap.getOrDefault(colName, colName);
						fields[i].abstraction = new Abstraction();
						fields[i].abstraction.aggregation = insight.subject.measures.get(i - 1).aggr.getEnName();
					}
				}
			}
		} else {
			for (int i = 0; i < colNum; i++) {
				fields[i] = new Field();
				fields[i].id = colNames[i];
				fields[i].code = colNames[i];
				if (i == 1) {
					if (insight.subject != null && insight.subject.measures != null) {
						String colName = insight.subject.measures.get(0).colName;
						fields[i].alias = cnNamesMap.getOrDefault(colName, colName);
						fields[i].abstraction = new Abstraction();
						fields[i].abstraction.aggregation = insight.subject.measures.get(0).aggr.getEnName();
					}
				}
			}
		}

		for (int i = 0; i < colNum; i++) {
			if (TableUtil.isSupportedNumericType(colTypes[i])) {
				fields[i].type = ColType.numerical;
			} else if (TableUtil.isSupportedDateType(colTypes[i])) {
				fields[i].type = ColType.temporal;
			} else {
				fields[i].type = ColType.categorical;
			}
		}

		Data data = new Data();
		data.content = new HashMap[rowNum];
		for (int i = 0; i < rowNum; i++) {
			data.content[i] = new HashMap <String, Object>();
			for (int j = 0; j < colNum; j++) {
				Object obj = insight.layout.data.getEntry(i, j);
				data.content[i].put(colNames[j], insight.layout.data.getEntry(i, j));
				if (obj != null) {
					if (obj instanceof Timestamp) {
						data.content[i].put(colNames[j], sdf.format(insight.layout.data.getEntry(i, j)));
					}
				}
			}
		}

		DvInsightDescription description = new DvInsightDescription();

		Meta meta = new Meta();
		meta.score = insight.score;
		meta.fields = fields;
		meta.data = data;

		Target target = new Target();
		DatavFilter[] datavFilters = null;
		if (insight.subject != null && insight.subject.subspaces != null && !insight.subject.subspaces.isEmpty()) {
			target.subspace = new Subspace[1];
			target.subspace[0] = new Subspace();
			target.subspace[0].fieldId = insight.subject.subspaces.get(0).colName;
			target.subspace[0].range = new String[] {String.valueOf(insight.subject.subspaces.get(0).value)};

			datavFilters = new DatavFilter[1];
			datavFilters[0] = new DatavFilter();
			datavFilters[0].code = insight.subject.subspaces.get(0).colName;
			datavFilters[0].values = new String[] {String.valueOf(insight.subject.subspaces.get(0).value)};
		}
		if (insight.subject != null && insight.subject.breakdown != null) {
			target.breakdowns = new Breakdown[1];
			target.breakdowns[0] = new Breakdown();
			target.breakdowns[0].fieldId = insight.subject.breakdown.colName;
		}

		target.measures = new Measure[1];
		target.measures[0] = new Measure();

		target.measures[0].fieldId = colNames[1];

		Result result = new Result();
		result.target = target;
		result.text = insight.layout.description;
		result.factType = insight.type.name();

		View view = new View();
		view.attributes = colNames;
		view.subspaces = datavFilters;

		switch (insight.type) {
			case Attribution:
			case OutstandingNo1:
			case OutstandingTop2:
				view.tasks = new Task[2];
				view.tasks[0] = new Task();
				view.tasks[0].type = "summarize.ratio";
				view.tasks[0].dimensions = new String[] {colNames[0]};
				view.tasks[0].measures = new String[] {colNames[1]};
				view.tasks[1] = new Task();
				view.tasks[1].type = "find.extreme.max";
				view.tasks[1].dimensions = new String[] {colNames[0]};
				view.tasks[1].measures = new String[] {colNames[1]};
				if (insight.layout.focus != null) {
					view.tasks[1].focuses = new String[insight.layout.focus.length][1];
					for (int i = 0; i < insight.layout.focus.length; i++) {
						view.tasks[1].focuses[i][0] = insight.layout.focus[i];
					}
				}
				break;
			case OutstandingLast:
				view.tasks = new Task[2];
				view.tasks[0] = new Task();
				view.tasks[0].type = "summarize.ratio";
				view.tasks[0].dimensions = new String[] {colNames[0]};
				view.tasks[0].measures = new String[] {colNames[1]};
				view.tasks[1] = new Task();
				view.tasks[1].type = "find.extreme.min";
				view.tasks[1].dimensions = new String[] {colNames[0]};
				view.tasks[1].measures = new String[] {colNames[1]};
				view.tasks[1].focuses = new String[][] {insight.layout.focus};
				break;
			case Evenness:
				view.tasks = new Task[0];
				break;
			case ChangePoint:
			case Outlier:
				view.tasks = new Task[2];
				view.tasks[0] = new Task();
				view.tasks[0].type = "summarize.trend";
				view.tasks[0].dimensions = new String[] {colNames[0]};
				view.tasks[0].measures = new String[] {colNames[1]};
				view.tasks[1] = new Task();
				view.tasks[1].type = "find.anomalies";
				view.tasks[1].dimensions = new String[] {colNames[0]};
				view.tasks[1].measures = new String[] {colNames[1]};
				view.tasks[1].focuses = new String[][] {insight.layout.focus};
				break;
			case Seasonality:
				view.tasks = new Task[1];
				view.tasks[0] = new Task();
				view.tasks[0].type = "summarize.seasonality";
				view.tasks[0].dimensions = new String[] {colNames[0]};
				view.tasks[0].measures = new String[] {colNames[1]};
				// todo
				view.tasks[0].seasonality = insight.layout.focus;
				break;
			case Trend:
				view.tasks = new Task[1];
				view.tasks[0] = new Task();
				view.tasks[0].type = "summarize.trend";
				view.tasks[0].dimensions = new String[] {colNames[0]};
				view.tasks[0].measures = new String[] {colNames[1]};

				String[] lineA = insight.layout.lineA.split(",");
				double a = Double.parseDouble(lineA[1]);
				double b = Double.parseDouble(lineA[0]);

				view.tasks[0].trend = new Trend();
				view.tasks[0].trend.from = new Object[2];
				view.tasks[0].trend.from[0] = sdf.format(insight.layout.data.getEntry(0, 0));
				view.tasks[0].trend.from[1] = b;
				view.tasks[0].trend.to = new Object[2];
				view.tasks[0].trend.to[0] = sdf.format(
					insight.layout.data.getEntry(insight.layout.data.getNumRow() - 1, 0));
				view.tasks[0].trend.to[1] = a * insight.layout.data.getNumRow() + b;
				break;
			case BasicStat:
				view.tasks = new Task[1];
				view.tasks[0] = new Task();
				view.tasks[0].type = "summarize.value";
				view.tasks[0].dimensions = new String[] {};
				view.tasks[0].measures = new String[colNum - 1];
				System.arraycopy(colNames, 1, view.tasks[0].measures, 0, colNum - 1);

				if (colNames.length == 2) {
					view.attributes = new String[]{colNames[0]};
					view.tasks[0].measures = new String[]{colNames[0]};
				}

				break;
			case Distribution:
				view.tasks = new Task[1];
				view.tasks[0] = new Task();
				view.tasks[0].type = "summarize.distribution";
				view.tasks[0].dimensions = new String[] {colNames[0]};
				view.tasks[0].measures = new String[colNum - 1];
				for (int i = 1; i < colNum; i++) {
					view.tasks[0].measures[i - 1] = colNames[i];
				}
			case CrossMeasureCorrelation:
			case Correlation:
				view.tasks = new Task[1];
				view.tasks[0] = new Task();
				view.tasks[0].type = "correlation.measure";
				view.tasks[0].dimensions = new String[] {colNames[0]};
				view.tasks[0].measures = new String[colNum - 1];
				System.arraycopy(colNames, 1, view.tasks[0].measures, 0, colNum - 1);
				break;
			case Clustering2D:
				view.tasks = new Task[1];
				view.tasks[0] = new Task();
				view.tasks[0].type = "correlation.measure";
				view.tasks[0].dimensions = new String[] {colNames[0]};
				view.tasks[0].measures = new String[colNum - 2];
				for (int i = 1; i < colNum - 1; i++) {
					view.tasks[0].measures[i - 1] = colNames[i];
				}
				view.attributes = new String[colNames.length - 1];
				System.arraycopy(colNames, 0, view.attributes, 0, view.attributes.length);

				break;
		}

		description.meta = meta;
		description.result = result;
		description.view = view;

		return description;
	}
}
