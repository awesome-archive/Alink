package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.HasHandleInvalid;
import com.alibaba.alink.params.shared.HasNumThreads;
import com.alibaba.alink.params.shared.colname.HasOutputCols;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;
import com.alibaba.alink.params.timeseries.HasEntryFunction;
import com.alibaba.alink.params.timeseries.HasFunctionParams;
import com.alibaba.alink.params.timeseries.HasUdfFilePath;

public interface GroupPandasUdfParams<T> extends
	HasGroupCols <T>,
	HasSelectedColsDefaultAsNull <T>,
	HasOutputCols <T>,
	HasReservedColsDefaultAsNull <T>,
	HasHandleInvalid <T>,
	HasNumThreads <T>,
	HasUdfFilePath <T>,
	HasFunctionParams <T>,
	HasEntryFunction <T> {
}
