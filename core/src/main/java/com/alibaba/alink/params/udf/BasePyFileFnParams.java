package com.alibaba.alink.params.udf;

import com.alibaba.alink.params.timeseries.HasFunctionParams;

public interface BasePyFileFnParams<T> extends
	HasClassName <T>,
	HasUserFilePaths <T>,
	HasFunctionParams <T>,
	HasPythonVersion <T>,
	HasPythonEnvFilePath <T> {
}
