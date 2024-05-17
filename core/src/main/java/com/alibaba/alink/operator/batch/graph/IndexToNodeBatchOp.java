package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.SelectedColsWithSecondInputSpec;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.graph.IndexToNodeModelMapper;
import com.alibaba.alink.params.graph.IndexToNodeParams;

/**
 * index to node.
 */
@SelectedColsWithSecondInputSpec
@NameCn("索引值转换为节点值")
@NameEn("Index to Node")
public class IndexToNodeBatchOp extends ModelMapBatchOp <IndexToNodeBatchOp>
	implements IndexToNodeParams <IndexToNodeBatchOp> {

	public IndexToNodeBatchOp() {
		this(null);
	}

	public IndexToNodeBatchOp(Params params) {
		super(IndexToNodeModelMapper::new, params);
	}
}




