package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.SelectedColsWithSecondInputSpec;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.graph.NodeToIndexModelMapper;
import com.alibaba.alink.params.graph.NodeToIndexParams;

/**
 * node to index.
 */
@SelectedColsWithSecondInputSpec
@NameCn("节点值转换为索引值")
@NameEn("Node to Index")
public class NodeToIndexBatchOp extends ModelMapBatchOp <NodeToIndexBatchOp>
	implements NodeToIndexParams <NodeToIndexBatchOp> {

	public NodeToIndexBatchOp() {
		this(null);
	}

	public NodeToIndexBatchOp(Params params) {
		super(NodeToIndexModelMapper::new, params);
	}
}




