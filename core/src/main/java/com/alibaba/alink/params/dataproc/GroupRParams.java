package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.params.dl.HasUserParams;
import com.alibaba.alink.params.shared.HasHandleInvalid;
import com.alibaba.alink.params.shared.colname.HasOutputCols;
import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;

public interface GroupRParams<T> extends
	HasGroupCols <T>,
	HasSelectedColsDefaultAsNull <T>,
	HasOutputCols <T>,
	HasHandleInvalid <T>,
	HasUserParams <T> {

	@NameCn("脚本路径")
	@DescCn("R脚本所在的位置")
	ParamInfo <String> FILE_PATH = ParamInfoFactory
		.createParamInfo("filePath", String.class)
		.setDescription("File path with file system.")
		.setRequired()
		.build();

	default FilePath getFilePath() {
		return new FilePath(get(FILE_PATH));
	}

	default T setFilePath(String value) {
		return set(FILE_PATH, new FilePath(value).serialize());
	}
}
