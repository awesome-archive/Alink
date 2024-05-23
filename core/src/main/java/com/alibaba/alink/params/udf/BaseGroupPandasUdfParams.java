package com.alibaba.alink.params.udf;

import com.alibaba.alink.params.dataproc.HasGroupCols;
import com.alibaba.alink.params.shared.HasHandleInvalid;

public interface BaseGroupPandasUdfParams<T> extends
	HasGroupCols <T>,
	HasHandleInvalid <T>,
	BasePandasUdfParams <T> {
}
