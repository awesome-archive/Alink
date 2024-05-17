package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.validators.MinValidator;

public interface CommunityDetectionClusterParams<T> extends
	CommonGraphParams <T>,
	HasEdgeWeightCol <T>,
	HasMaxIterDefaultAs50 <T>,
	HasVertexCol <T>,
	HasVertexWeightCol <T> {
}
