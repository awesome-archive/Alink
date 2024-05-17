package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;

public interface SimrankParams<T> extends
	HasItemCol <T>,
	HasUserCol <T>,
	HasWeightColDefaultAsNull <T> {

	@NameCn("随机游走次数")
	@DescCn("随机游走次数")
	ParamInfo <Integer> NUM_WALKS = ParamInfoFactory
		.createParamInfo("numWalks", Integer.class)
		.setDescription("num walks")
		.setHasDefaultValue(100)
		.build();

	default Integer getNumWalks() {
		return get(NUM_WALKS);
	}

	default T setNumWalks(Integer value) {
		return set(NUM_WALKS, value);
	}

	@NameCn("随机游走长度")
	@DescCn("随机游走长度")
	ParamInfo <Integer> WALK_LENGTH = ParamInfoFactory
		.createParamInfo("walkLength", Integer.class)
		.setDescription("walk length")
		.setHasDefaultValue(10)
		.build();

	default Integer getWalkLength() {
		return get(WALK_LENGTH);
	}

	default T setWalkLength(Integer value) {
		return set(WALK_LENGTH, value);
	}

	@NameCn("simrank模型类型")
	@DescCn("simrank模型类型， \"simrank\"或\"simrankplusplus\"")
	ParamInfo <ModelType> MODEL_TYPE = ParamInfoFactory
		.createParamInfo("modelType", ModelType.class)
		.setDescription("model type")
		.setHasDefaultValue(ModelType.SIMRANKPLUSPLUS)
		.build();

	default ModelType getModelType() {
		return get(MODEL_TYPE);
	}

	default T setModelType(ModelType value) {
		return set(MODEL_TYPE, value);
	}

	default T setModelType(String value) {
		return set(MODEL_TYPE, ParamUtil.searchEnum(MODEL_TYPE, value));
	}

	enum ModelType {
		SIMRANK,
		SIMRANKPLUSPLUS
	}

	@NameCn("衰减因子")
	@DescCn("衰减因子")
	ParamInfo <Double> DECAY_FACTOR = ParamInfoFactory
		.createParamInfo("decayFactor", Double.class)
		.setDescription("decay factor")
		.setHasDefaultValue(0.8)
		.build();

	default Double getDecayFactor() {
		return get(DECAY_FACTOR);
	}

	default T setDecayFactor(Double value) {
		return set(DECAY_FACTOR, value);
	}

	@NameCn("topK")
	@DescCn("表示取前topK个相似的item进行输出")
	ParamInfo <Integer> TOP_K = ParamInfoFactory
		.createParamInfo("topK", Integer.class)
		.setDescription("topK")
		.setHasDefaultValue(100)
		.build();

	default Integer getTopK() {
		return get(TOP_K);
	}

	default T setTopK(Integer value) {
		return set(TOP_K, value);
	}

}
