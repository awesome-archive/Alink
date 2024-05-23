package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.MTableUtil;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.pyrunner.pandas.PyDataFrameCalcRunner;
import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.utils.CloseableThreadLocal;
import com.alibaba.alink.operator.common.utils.UDFHelper;
import com.alibaba.alink.params.timeseries.HasFunctionParams;
import com.alibaba.alink.params.timeseries.PandasUdfParams;
import com.alibaba.alink.params.udf.BasePyBinaryFnParams;
import com.alibaba.alink.params.udf.BasePyFileFnParams;
import com.alibaba.alink.params.udf.HasClassObject;
import com.alibaba.alink.params.udf.HasUserFilePaths;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PandasUdfMapper extends Mapper {

	private static final String USER_PARAMS_KEY = "user_params";

	private transient CloseableThreadLocal <PyDataFrameCalcRunner> runner;

	public PandasUdfMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	@Override
	public void open() {
		super.open();
		this.runner = new CloseableThreadLocal <>(this::createPythonRunner, this::destroyPythonRunner);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		int len = selection.length();
		List <MTable> inputs = new ArrayList <>();
		for (int i = 0; i < len; i++) {
			inputs.add(MTableUtil.getMTable(selection.get(i)));
		}
		List <MTable> outputs = runner.get().calc(inputs);
		for (int i = 0; i < outputs.size(); i++) {
			result.set(i, outputs.get(i));
		}
	}

	/**
	 * Returns the tuple of selectedCols, resultCols, resultTypes, reservedCols.
	 *
	 * @param dataSchema
	 * @param params
	 */
	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema dataSchema,
																						   Params params) {
		String[] outputColsName = params.get(PandasUdfParams.OUTPUT_COLS);
		TypeInformation[] outputColsType = new TypeInformation[outputColsName.length];
		Arrays.fill(outputColsType, AlinkTypes.M_TABLE);
		return Tuple4.of(params.get(PandasUdfParams.SELECTED_COLS),
			outputColsName, outputColsType,
			params.get(PandasUdfParams.RESERVED_COLS));
	}

	private PyDataFrameCalcRunner createPythonRunner() {
		JsonObject fnSpec;
		Map <String, String> runConfig;
		if (params.contains(HasUserFilePaths.USER_FILE_PATHS)) {
			fnSpec = UDFHelper.makeFnSpec((BasePyFileFnParams <?>) () -> params);
			runConfig = UDFHelper.makeRunConfig((BasePyFileFnParams <?>) () -> params);
		} else if (params.contains(HasClassObject.CLASS_OBJECT)) {
			fnSpec = UDFHelper.makeFnSpec((BasePyBinaryFnParams <?>) () -> params);
			runConfig = UDFHelper.makeRunConfig((BasePyBinaryFnParams <?>) () -> params);
		} else {
			throw new AkUnclassifiedErrorException("Unsupported type: " + params.getClass().getSimpleName());
		}

		if (params.contains(HasFunctionParams.FUNCTION_PARAMS)) {
			runConfig.put(USER_PARAMS_KEY, params.get(HasFunctionParams.FUNCTION_PARAMS));
		}

		PyDataFrameCalcRunner runner = new PyDataFrameCalcRunner(fnSpec.toString(), runConfig);

		runner.open();

		return runner;
	}

	private void destroyPythonRunner(PyDataFrameCalcRunner runner) {
		runner.close();
	}

	@Override
	public void close() {
		this.runner.close();
	}

}


