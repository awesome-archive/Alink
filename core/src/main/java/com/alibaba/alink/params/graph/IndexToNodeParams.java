package com.alibaba.alink.params.graph;

import com.alibaba.alink.params.mapper.ModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasOutputColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

public interface IndexToNodeParams<T> extends
	ModelMapperParams <T>,
	HasSelectedCols <T>,
	HasReservedColsDefaultAsNull <T>,
	HasOutputColsDefaultAsNull <T> {
}
