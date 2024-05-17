package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerTrainBatchOp;
import com.alibaba.alink.operator.common.recommendation.SimrankImpl;
import com.alibaba.alink.params.recommendation.SimrankParams;

import java.util.List;

/**
 * Simrank is a calc measure for nodes on a graph.
 * Reference: Ioannis Antonellis, Simrank++: Query Rewriting through Link Analysis of the Click Graph
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ParamSelectColumnSpec(name = "itemCol")
@ParamSelectColumnSpec(name = "userCol")
@ParamSelectColumnSpec(name = "weightCol", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("simrank")
@NameEn("simrank")
public final class SimrankBatchOp
	extends BatchOperator <SimrankBatchOp>
	implements SimrankParams <SimrankBatchOp> {

	private static final long serialVersionUID = -6593373043441977811L;

	public SimrankBatchOp() {
		this(new Params());
	}

	public SimrankBatchOp(Params params) {
		super(params);
	}

	@Override
	public SimrankBatchOp linkFrom(List <BatchOperator <?>> ins) {
		return linkFrom(ins.get(0));
	}

	@Override
	public SimrankBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		String userColName = getUserCol();
		String itemColName = getItemCol();
		String weightColName = getWeightCol();
		final boolean hasWeightCol = !StringUtils.isNullOrWhitespaceOnly(weightColName);

		if (hasWeightCol) {
			in = in.select(new String[] {userColName, itemColName, weightColName});
		} else {
			in = in.select(new String[] {userColName, itemColName});
		}

		MultiStringIndexerTrainBatchOp indexer = new MultiStringIndexerTrainBatchOp()
			.setMLEnvironmentId(getMLEnvironmentId())
			.setSelectedCols(userColName, itemColName);
		indexer.linkFrom(in);

		MultiStringIndexerPredictBatchOp indexerPredictor = new MultiStringIndexerPredictBatchOp()
			.setMLEnvironmentId(getMLEnvironmentId())
			.setSelectedCols(userColName, itemColName);
		in = indexerPredictor.linkFrom(indexer, in);

		final int userColIdx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), userColName);
		final int itemColIdx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), itemColName);
		final int weightColIdx = hasWeightCol ? TableUtil.findColIndexWithAssertAndHint(in.getColNames(),
			weightColName)
			: -1;

		// tuple3: userId, itemId, weight
		DataSet <Tuple3 <Integer, Integer, Float>> simrankInput = in
			.getDataSet()
			.map(new MapFunction <Row, Tuple3 <Integer, Integer, Float>>() {
				private static final long serialVersionUID = -7653517928304229580L;

				@Override
				public Tuple3 <Integer, Integer, Float> map(Row value) throws Exception {
					int userId = ((Long) value.getField(userColIdx)).intValue();
					int itemId = ((Long) value.getField(itemColIdx)).intValue();
					float weight = 1.0F;
					if (hasWeightCol) {
						((Number) value.getField(weightColIdx)).floatValue();
					}
					return Tuple3.of(userId, itemId, weight);
				}
			});

		SimrankImpl simrankImpl = new SimrankImpl(this.getParams());

		DataSet <Tuple3 <Integer, int[], float[]>> result = simrankImpl.batchPredict(simrankInput);
		result = result.filter(new FilterFunction <Tuple3 <Integer, int[], float[]>>() {
			private static final long serialVersionUID = 1346817402754316509L;

			@Override
			public boolean filter(Tuple3 <Integer, int[], float[]> value) throws Exception {
				return value.f1.length > 0;
			}
		});

		DataSet <Tuple2 <String, Integer>> tokenMap = extractMultiStringIndexerModel(indexer);

		DataSet <Tuple3 <String, String, String>> output = mapIndexToTokens(result, tokenMap);
		DataSet <Row> outputRows = output
			.map(new MapFunction <Tuple3 <String, String, String>, Row>() {
				private static final long serialVersionUID = -1252041500786707614L;

				@Override
				public Row map(Tuple3 <String, String, String> value) throws Exception {
					return Row.of(value.f0, value.f1, value.f2);
				}
			});

		setOutput(outputRows, new String[] {itemColName, "similar", "score"},
			new TypeInformation[] {Types.STRING, Types.STRING, Types.STRING});
		return this;
	}

	public static DataSet <Tuple2 <String, Integer>>
	extractMultiStringIndexerModel(BatchOperator model) {
		DataSet <Row> modelRows = model.getDataSet();
		return modelRows
			.flatMap(new RichFlatMapFunction <Row, Tuple2 <String, Integer>>() {
				private static final long serialVersionUID = 8098958902674167390L;

				@Override
				public void flatMap(Row row, Collector <Tuple2 <String, Integer>> out) throws Exception {
					Long colIdx = (Long) row.getField(0);
					if (colIdx == 1) {
						out.collect(Tuple2.of((String) row.getField(1),
							((Long) row.getField(2)).intValue()));
					}
				}
			});
	}

	private static DataSet <Tuple3 <String, String, String>>
	mapIndexToTokens(DataSet <Tuple3 <Integer, int[], float[]>> result, DataSet <Tuple2 <String, Integer>> tokenMap) {
		return result
			.map(new RichMapFunction <Tuple3 <Integer, int[], float[]>, Tuple3 <String, String, String>>() {
				private static final long serialVersionUID = 830024957269191692L;
				transient String[] tokens;

				@Override
				public void open(Configuration parameters) throws Exception {
					List <Tuple2 <String, Integer>> bc = getRuntimeContext().getBroadcastVariable("tokenMap");
					tokens = new String[bc.size()];
					bc.forEach(t2 -> {
						tokens[t2.f1] = t2.f0;
					});
				}

				@Override
				public Tuple3 <String, String, String> map(Tuple3 <Integer, int[], float[]> value) throws Exception {
					StringBuilder sbd1 = new StringBuilder();
					StringBuilder sbd2 = new StringBuilder();
					for (int i = 0; i < value.f1.length; i++) {
						if (i > 0) {
							sbd1.append(":");
							sbd2.append(":");
						}
						sbd1.append(tokens[value.f1[i]]);
						sbd2.append(value.f2[i]);
					}
					return Tuple3.of(tokens[value.f0], sbd1.toString(), sbd2.toString());
				}
			})
			.withBroadcastSet(tokenMap, "tokenMap");
	}
}

