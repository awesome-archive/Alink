package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface CommunityDetectionClassifyParams<T> extends
	CommonGraphParams <T>,
	HasEdgeWeightCol <T>,
	HasMaxIterDefaultAs50 <T>,
	HasVertexCol <T>,
	HasVertexWeightCol <T> {

	@NameCn("输入点表中标签所在列")
	@DescCn("输入点表中标签所在列")
	ParamInfo <String> VERTEX_LABEL_COL = ParamInfoFactory
		.createParamInfo("vertexLabelCol", String.class)
		.setDescription("vertex Label Col")
		.setRequired()
		.build();

	default String getVertexLabelCol() {return get(VERTEX_LABEL_COL);}

	default T setVertexLabelCol(String value) {return set(VERTEX_LABEL_COL, value);}

	@NameCn("delta")
	@DescCn("delta参数")
	ParamInfo <Double> DELTA = ParamInfoFactory
		.createParamInfo("delta", Double.class)
		.setDescription("delta param")
		.setHasDefaultValue(0.2)
		.build();

	default Double getDelta() {return get(DELTA);}

	default T setDelta(Double value) {return set(DELTA, value);}
}
