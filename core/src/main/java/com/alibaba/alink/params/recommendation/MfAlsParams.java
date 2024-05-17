package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface MfAlsParams<T> extends
	AlsImplicitTrainParams <T> {

	@NameCn("是否采用隐式偏好模型")
	@DescCn("是否采用隐式偏好模型")
	ParamInfo <Boolean> IMPLICIT_PREFS = ParamInfoFactory
		.createParamInfo("implicitPrefs", Boolean.class)
		.setDescription("Whether to use implicit preference model.")
		.setHasDefaultValue(false)
		.setAlias(new String[] {"implicitPref"})
		.build();

	default T setImplicitPrefs(Boolean value) {
		return set(IMPLICIT_PREFS, value);
	}

}
