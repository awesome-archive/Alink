package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.AppendIdBatchOp;
import com.alibaba.alink.operator.batch.sql.UnionBatchOp;
import com.alibaba.alink.params.graph.NodeIndexerTrainParams;

@InputPorts(values = @PortSpec(value = PortType.DATA))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@NameCn("NodeIndexer训练")
@NameEn("train NodeIndexer")
public class NodeIndexerTrainBatchOp extends BatchOperator <NodeIndexerTrainBatchOp>
	implements NodeIndexerTrainParams <NodeIndexerTrainBatchOp> {
	public NodeIndexerTrainBatchOp() {
		this(new Params());
	}

	public NodeIndexerTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public NodeIndexerTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> input = checkAndGetFirst(inputs);

		TableSchema schema = input.getSchema();
		String[] selectedColNames = getSelectedCols();
		if (null == selectedColNames) {
			selectedColNames = schema.getFieldNames();
		}

		TypeInformation <?>[] selectedColTypes = TableUtil.findColTypes(schema, selectedColNames);
		for (int i = 1; i < selectedColTypes.length; i++) {
			if (!selectedColTypes[0].equals(selectedColTypes[i])) {
				throw new RuntimeException("所有选中的列必须使用同样的数据类型。");
			}
		}

		BatchOperator <?>[] inputCols = new BatchOperator[selectedColNames.length];
		for (int i = 0; i < selectedColNames.length; i++) {
			inputCols[i] = input.select(selectedColNames[i] + " AS node");
		}

		this.setOutputTable(
			new UnionBatchOp()
				.linkFrom(inputCols)
				.distinct()
				.link(
					new AppendIdBatchOp()
						.setIdCol("id")
				)
				.select("node, CAST(id AS INT) AS id")
				.getOutputTable()
		);

		return this;
	}
}
