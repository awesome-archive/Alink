package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.viz.AlinkViz;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.viz.VizDataWriterInterface;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.DataSetUtil;
import com.alibaba.alink.operator.common.feature.binning.Bins;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsCalculator;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsCalculatorTransformer;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsUtil;
import com.alibaba.alink.operator.common.finance.VizData;
import com.alibaba.alink.params.feature.HasEncode;
import com.alibaba.alink.params.finance.PSIParams;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {
	@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)
})
@ParamSelectColumnSpec(name = "selectedCols")
@NameCn("样本稳定指数")
@NameEn("PSI")
public final class PSIBatchOp extends BatchOperator <PSIBatchOp> implements PSIParams <PSIBatchOp>,
	AlinkViz <PSIBatchOp> {
	private static final long serialVersionUID = 5339111953189545988L;

	public PSIBatchOp() {
		super(null);
	}

	public PSIBatchOp(Params params) {
		super(params);
	}

	@Override
	public PSIBatchOp linkFrom(BatchOperator <?>... in) {
		checkOpSize(3, in);

		BinningPredictBatchOp indexBase = new BinningPredictBatchOp(getParams())
			.setEncode(HasEncode.Encode.INDEX)
			.linkFrom(in[0], in[1]);

		BinningPredictBatchOp indexTest = new BinningPredictBatchOp(getParams())
			.setEncode(HasEncode.Encode.INDEX)
			.linkFrom(in[0], in[2]);

		DataSet <FeatureBinsCalculator> featureBorderDataSet = FeatureBinsUtil.parseFeatureBinsModel(
			in[0].getDataSet());

		DataSet <FeatureBinsCalculator> featureBorderTotalBase = BinningTrainBatchOp.setFeatureBinsTotal(
			featureBorderDataSet,
			indexBase, getSelectedCols());

		DataSet <FeatureBinsCalculator> featureBorderTotalTest = BinningTrainBatchOp.setFeatureBinsTotal(
			featureBorderDataSet,
			indexTest, getSelectedCols());

		DataSet <VizData.PSIVizData> psi = featureBorderTotalBase
			.flatMap(new FeatureBinsToPSI(true))
			.join(featureBorderTotalTest.flatMap(new FeatureBinsToPSI(false)))
			.where(new PSIKey())
			.equalTo(new PSIKey())
			.with(new JoinFunction <VizData.PSIVizData, VizData.PSIVizData, VizData.PSIVizData>() {
				private static final long serialVersionUID = -4737602826178084970L;

				@Override
				public VizData.PSIVizData join(VizData.PSIVizData first, VizData.PSIVizData second) throws Exception {
					first.testPercentage = second.testPercentage;
					first.calcPSI();
					return first;
				}
			});
		psi = psi.union(psi
			.groupBy(new PSIFeatureNameKey())
			.reduceGroup(new GroupReduceFunction <VizData.PSIVizData, VizData.PSIVizData>() {
				private static final long serialVersionUID = -3650572309339461256L;

				@Override
				public void reduce(Iterable <VizData.PSIVizData> values, Collector <VizData.PSIVizData> out)
					throws Exception {
					double psi = 0.0;
					String featureName = null;
					for (VizData.PSIVizData data : values) {
						featureName = data.featureName;
						if (null != data.psi) {
							psi += data.psi;
						}
					}
					if (null != featureName) {
						VizData.PSIVizData psiData = new VizData.PSIVizData(featureName);
						psiData.psi = psi;
						out.collect(psiData);
					}
				}
			}));

		DataSet <Row> res = psi.map(new MapFunction <VizData.PSIVizData, Row>() {
			private static final long serialVersionUID = -2993050177290130955L;

			@Override
			public Row map(VizData.PSIVizData value) throws Exception {
				return Row.of(value.featureName, value.index, value.value, value.testPercentage, value.basePercentage,
					value.testSubBase, value.lnTestDivBase, value.psi);
			}
		});

		VizDataWriterInterface writer = this.getVizDataWriter();
		if (writer != null) {
			DataSet <Row> dummy = psi.mapPartition(new VizDataWriter(writer)).setParallelism(1).name("WriteVizData");
			DataSetUtil.linkDummySink(dummy);
		}

		this.setOutput(res, OUTPUT_COLS, OUTPUT_TYPES);
		return this;
	}

	public static class PSIFeatureNameKey implements KeySelector <VizData.PSIVizData, String> {
		private static final long serialVersionUID = 6803237152351509469L;

		@Override
		public String getKey(VizData.PSIVizData value) {
			return value.featureName;
		}
	}

	public static class PSIKey implements KeySelector <VizData.PSIVizData, String> {
		private static final long serialVersionUID = 8375105176924537427L;

		@Override
		public String getKey(VizData.PSIVizData value) {
			return value.featureName + "," + value.index;
		}
	}

	public static class FeatureBinsToPSI implements FlatMapFunction <FeatureBinsCalculator, VizData.PSIVizData> {
		private static final long serialVersionUID = -8690273954521768521L;
		private boolean isBase;

		public FeatureBinsToPSI(boolean isBase) {
			this.isBase = isBase;
		}

		@Override
		public void flatMap(FeatureBinsCalculator featureBinsCalculator, Collector <VizData.PSIVizData> out) {
			FeatureBinsCalculatorTransformer.toFeatureBins(featureBinsCalculator);
			featureBinsCalculator.splitsArrayToInterval();
			if (null != featureBinsCalculator.bin.nullBin) {
				out.collect(transform(featureBinsCalculator.getFeatureName(), featureBinsCalculator.bin.nullBin,
					FeatureBinsUtil.NULL_LABEL));
			}
			if (null != featureBinsCalculator.bin.elseBin) {
				out.collect(transform(featureBinsCalculator.getFeatureName(), featureBinsCalculator.bin.elseBin,
					FeatureBinsUtil.ELSE_LABEL));
			}
			if (null != featureBinsCalculator.bin.normBins) {
				for (Bins.BaseBin bin : featureBinsCalculator.bin.normBins) {
					out.collect(
						transform(
							featureBinsCalculator.getFeatureName(),
							bin,
							bin.getValueStr(featureBinsCalculator.getColType())));
				}

			}
		}

		private VizData.PSIVizData transform(String featureName, Bins.BaseBin bin, String label) {
			VizData.PSIVizData psiData = new VizData.PSIVizData(featureName, bin.getIndex(), label);
			if (isBase) {
				psiData.basePercentage = bin.getTotalRate() * 100;
			} else {
				psiData.testPercentage = bin.getTotalRate() * 100;
			}
			return psiData;
		}
	}

	private static String[] OUTPUT_COLS = new String[] {"featureName", "index", "values", "testPercentage",
		"basePercentage",
		"testSubBase", "lnTestDivBase", "psi"};
	private static TypeInformation[] OUTPUT_TYPES = new TypeInformation[] {Types.STRING, Types.LONG, Types.STRING,
		Types.DOUBLE,
		Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE};

	private static class VizDataWriter implements MapPartitionFunction <VizData.PSIVizData, Row> {
		private static final long serialVersionUID = -4150103061121224827L;
		private VizDataWriterInterface writer;

		public VizDataWriter(VizDataWriterInterface writer) {
			this.writer = writer;
		}

		@Override
		public void mapPartition(Iterable <VizData.PSIVizData> psiData, Collector <Row> collector) throws Exception {

			Map <String, TreeSet <VizData.PSIVizData>> map = new HashMap <>();
			for (VizData.PSIVizData data : psiData) {
				if (null != data.index) {
					TreeSet <VizData.PSIVizData> list = map.computeIfAbsent(data.featureName,
						k -> new TreeSet <>(VizData.VizDataComparator));
					list.add(data);
				}
			}
			writer.writeBatchData(0L, JsonConverter.toJson(map), System.currentTimeMillis());
		}
	}
}
