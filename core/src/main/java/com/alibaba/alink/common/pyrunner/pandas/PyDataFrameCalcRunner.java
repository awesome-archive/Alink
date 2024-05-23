package com.alibaba.alink.common.pyrunner.pandas;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.pyrunner.PyCalcRunner;
import com.alibaba.alink.common.pyrunner.PyObjHandle;
import com.alibaba.alink.common.pyrunner.fn.PyFnUtils;
import com.alibaba.alink.common.pyrunner.pandas.PyDataFrameCalcRunner.PyDataFrameCalcHandler;
import com.alibaba.alink.common.utils.Functional.SerializableBiFunction;
import com.alibaba.alink.common.utils.JsonConverter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.common.pyrunner.bridge.BasePythonBridge.PY_TURN_ON_LOGGING_KEY;

public class PyDataFrameCalcRunner extends PyCalcRunner <List <MTable>, List <MTable>, PyDataFrameCalcHandler> {

	public static final String PY_CLASS_NAME = "alink.fn.PyDataFrameFn";

	private final ImmutableMap <String, String> config;

	private String fnSpecJson;

	public PyDataFrameCalcRunner(String fnSpecJson, final Map <String, String> config) {
		super(
			PY_CLASS_NAME,
			new SerializableBiFunction <String, String, String>() {
				@Override
				public String apply(String key, String defaultValue) {
					if (PY_TURN_ON_LOGGING_KEY.equals(key)) {
						return String.valueOf(AlinkGlobalConfiguration.isPrintProcessInfo());
					} else {
						return config.getOrDefault(key, defaultValue);
					}
				}
			}
		);

		this.fnSpecJson = fnSpecJson;
		this.config = ImmutableMap.copyOf(config);
	}

	@Override
	public void preOpenBridgeHook(Path workDir) {
		super.preOpenBridgeHook(workDir);
		fnSpecJson = PyFnUtils.downloadFilePaths(fnSpecJson, workDir);
	}

	@Override
	public void open() {
		super.open();
		handle.init(fnSpecJson);
	}

	@Override
	public List <MTable> calc(List <MTable> mTables) {
		List <String> contents = new ArrayList <>();
		for (MTable mTable : mTables) {
			try {
				StringWriter stringWriter = new StringWriter();
				BufferedWriter writer = new BufferedWriter(stringWriter);
				mTable.writeCsvToFile(writer);
				writer.flush();
				String content = stringWriter.toString();
				contents.add(content);
				writer.close();
				stringWriter.close();
			} catch (IOException e) {
				throw new AkIllegalDataException("Convert mtable to csv failed.", e);
			}
		}

		String[][] inputColNames = mTables.stream()
			.map(MTable::getColNames)
			.toArray(String[][]::new);

		HashMap <String, String> localConfig = Maps.newHashMap(config);
		localConfig.put("input_col_names", JsonConverter.toJson(inputColNames));

		PyDataFrameCollector collector = new PyDataFrameCollector();
		handle.setCollector(collector);
		handle.calc(localConfig, contents);

		return collector.getMTables();
	}

	public interface PyDataFrameCalcHandler extends PyObjHandle {

		void init(String fnSpecJson);

		void setCollector(PyDataFrameCollector collector);

		void calc(Map <String, String> config, List <String> dataframes);
	}

	/**
	 * Collect values from Python side as rows.
	 */
	public static class PyDataFrameCollector {
		private final List <MTable> mTables = new ArrayList <>();

		public void collectDataFrameFileName(String content, String schemaStr) throws IOException {
			StringReader stringReader = new StringReader(content);
			BufferedReader reader = new BufferedReader(stringReader);
			MTable mTable = MTable.readCsvFromFile(reader, schemaStr);
			mTables.add(mTable);
			reader.close();
			stringReader.close();
		}

		public List <MTable> getMTables() {
			return mTables;
		}
	}
}
