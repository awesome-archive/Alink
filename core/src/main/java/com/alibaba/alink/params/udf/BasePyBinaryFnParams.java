package com.alibaba.alink.params.udf;

import com.alibaba.alink.params.timeseries.HasFunctionParams;

public interface BasePyBinaryFnParams<T> extends
	HasClassObject <T>,
	HasClassObjectType <T>,
	HasFunctionParams<T>,
	HasPythonVersion <T>,
	HasPythonEnvFilePath <T> {
}
