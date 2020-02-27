package com.alibaba.alink.params.feature;

import com.alibaba.alink.operator.common.feature.binning.BinTypes;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasEncode<T> extends WithParams<T> {
    ParamInfo<String> ENCODE = ParamInfoFactory
        .createParamInfo("encode", String.class)
        .setDescription("encode type: INDEX, VECTOR, ASSEMBLED_VECTOR, WOE.")
        .setHasDefaultValue(BinTypes.Encode.ASSEMBLED_VECTOR.name())
        .build();

    default String getEncode() {
        return get(ENCODE);
    }

    default T setEncode(String value) {
        return set(ENCODE, value);
    }
}
