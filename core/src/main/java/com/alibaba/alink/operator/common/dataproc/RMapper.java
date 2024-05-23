package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.dl.utils.ArchivesUtils;
import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.MTableUtil;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.io.filesystem.BaseFileSystem;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.plugin.OsType;
import com.alibaba.alink.common.io.plugin.OsUtils;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import com.alibaba.alink.common.io.plugin.ResourcePluginFactory;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.CloseableThreadLocal;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.dataproc.GroupRParams;
import com.alibaba.alink.params.dataproc.RUdfParams;
import org.apache.commons.io.FileUtils;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPDouble;
import org.rosuda.REngine.REXPInteger;
import org.rosuda.REngine.REXPString;
import org.rosuda.REngine.RList;
import org.rosuda.REngine.Rserve.RConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

public class RMapper extends Mapper {

	private static final Logger LOG = LoggerFactory.getLogger(RMapper.class);

	private transient CloseableThreadLocal <Tuple2 <RConnection, Integer>> runner;

	private final ResourcePluginFactory factory;

	public RMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		factory = new ResourcePluginFactory();
	}

	@Override
	public void open() {
		this.runner = new CloseableThreadLocal <>(this::createRConnection, this::destroyRConnection);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		//assign data
		int len = selection.length();
		MTable[] inputs = new MTable[len];
		for (int i = 0; i < len; i++) {
			inputs[i] = MTableUtil.getMTable(selection.get(i));
		}

		for (int i = 0; i < len; i++) {
			runner.get().f0.assign("data_" + i, mTableToRDF(inputs[i]));
		}

		StringBuilder sbd = new StringBuilder()
			.append("func(")
			.append("data_0");

		for (int i = 1; i < len; i++) {
			sbd.append(", ")
				.append("data_")
				.append(i);
		}

		if (params.contains(GroupRParams.USER_PARAMS)) {
			sbd.append(", params)");
		} else {
			sbd.append(")");
		}

		LOG.info("expr: {}", sbd.toString());

		REXP rResponse = rEval(runner.get().f0, "eval(" + sbd.toString() + ")");

		MTable[] rt = rDfToMTables(rResponse, params.get(GroupRParams.OUTPUT_COLS));

		for (int i = 0; i < rt.length; i++) {
			result.set(i, rt[i]);
		}

	}

	private static REXP rEval(RConnection conn, String evalCmd) {
		try {
			REXP rResponse = conn.parseAndEval(String.format("try(%s,silent=TRUE)", evalCmd));
			if (rResponse.inherits("try-error")) {
				throw new RuntimeException("R Serve Eval Exception : " + rResponse.asString());
			}
			return rResponse;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema dataSchema,
																						   Params params) {
		String[] outputColsName = params.get(RUdfParams.OUTPUT_COLS);
		TypeInformation <?>[] outputColsType = new TypeInformation[outputColsName.length];
		Arrays.fill(outputColsType, AlinkTypes.M_TABLE);
		return Tuple4.of(
			new String[] {params.get(RUdfParams.SELECTED_COL)},
			outputColsName, outputColsType,
			params.get(RUdfParams.RESERVED_COLS));
	}

	private Tuple2 <RConnection, Integer> createRConnection() {
		LOG.info("start createRConnection.");

		//prepare plugin.
		OsType systemType = OsUtils.getSystemType();
		//String rEnv = params.get(RUdfParams.PYTHON_ENV);
		FilePath pluginFilePath = null;

		//if (null != rEnv) {
		//	if (PythonFileUtils.isCompressedFile(rEnv)) {
		//		String tempWorkDir = PythonFileUtils.createTempDir("r_env_").toString();
		//		ArchivesUtils.downloadDecompressToDirectory(rEnv, new File(tempWorkDir));
		//		String rEnv2 =
		//			new File(tempWorkDir, PythonFileUtils.getCompressedFileName(rEnv)).getAbsolutePath();
		//		pluginFilePath = new FilePath(rEnv2);
		//	} else {
		//		if (PythonFileUtils.isLocalFile(rEnv)) {
		//			String rEnv2 = rEnv.substring("file://".length());
		//			pluginFilePath = new FilePath(rEnv2);
		//		}
		//	}
		//} else {
			if (AlinkGlobalConfiguration.getAutoPluginDownload()) {
				try {
					pluginFilePath = factory.getResourcePluginPath(
						new RegisterKey("r_" + systemType.name().toLowerCase(), "0.01"));
				} catch (Exception ex) {
					LOG.info("plugin r_0.01 not found.");
				}
			}
		//}

		//start Rserve
		RserveCallable callable = null;
		int port = 0;
		try {
			//set Rserve
			switch (systemType) {
				case MACOSX:
					replaceBinFileMac(pluginFilePath);
					break;
				case LINUX:
					replaceBinFileLinux(pluginFilePath);
					break;
				default:
					throw new RuntimeException("system not support. " + systemType.name());
			}
			//get random available port.
			ServerSocket s = new ServerSocket(0);
			port = s.getLocalPort();
			s.close();

			LOG.info("listening on port: " + port);

			callable = new RserveCallable(pluginFilePath.getPathStr(), port);
			FutureTask <Integer> futureTask = new FutureTask <>(callable);
			new Thread(futureTask).start();

			//wait for Rserve start ok.
			while (callable.status == 1) {
				Thread.sleep(1000);
			}

			if (callable.status == -1) {
				s = new ServerSocket(0);
				port = s.getLocalPort();
				futureTask = new FutureTask <>(callable);
				new Thread(futureTask).start();
			}

			LOG.info("Rserve start ok.");
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}

		//r file
		try {
			String remoteRFileName = FilePath.deserialize(params.get(GroupRParams.FILE_PATH)).getPathStr();
			String localTempRFilePath = PythonFileUtils.createTempDir("r_env_").toString() + "/r_file";

			LOG.info("remoteRFileName: " + remoteRFileName);
			LOG.info("localTempRFilePath: " + localTempRFilePath);

			BaseFileSystem <?> fileSystem = new FilePath(remoteRFileName).getFileSystem();

			try (InputStream inputStream = fileSystem.open(remoteRFileName)) {
				FileUtils.copyInputStreamToFile(inputStream, new File(localTempRFilePath));
			}

			LOG.info("connection begin.");

			//connection
			RConnection connection = new RConnection("127.0.0.1", port);

			LOG.info("connection end.");

			connection.assign("fileName", localTempRFilePath);
			connection.eval("source(fileName)");

			String params = this.params.get(GroupRParams.USER_PARAMS);
			connection.assign("params", params);

			LOG.info("end createRConnection.");

			return Tuple2.of(connection, port);
		} catch (
			Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	private void replaceBinFileMac(FilePath filePath) throws Exception {
		String rBinFile = filePath.getPathStr() + "/R";
		String originStr = "tmp.1";
		String replaceStr = filePath.getPathStr() + "/";
		writeFile(readFile(rBinFile).replace(originStr, replaceStr), rBinFile);
	}

	private void replaceBinFileLinux(FilePath filePath) throws Exception {
		String rBinFile = filePath.getPathStr() + "/bin/R";
		String originStr = "tmp.1/";
		String replaceStr = filePath.getPathStr() + "/";
		writeFile(readFile(rBinFile).replace(originStr, replaceStr), rBinFile);

		String rLIB64BinFile = filePath.getPathStr() + "/lib64/R/bin/R";
		writeFile(readFile(rLIB64BinFile).replace(originStr, replaceStr), rLIB64BinFile);
	}

	private String readFile(String filePath) throws Exception {
		BufferedReader bufferedReader = null;
		StringBuffer buffer = null;
		bufferedReader = new BufferedReader(new FileReader(filePath));
		buffer = new StringBuffer();

		String temp = "";
		while ((temp = bufferedReader.readLine()) != null) {
			buffer.append(temp).append("\n");
		}
		bufferedReader.close();
		return buffer.toString();
	}

	private void writeFile(String text, String filePath) throws Exception {
		File file = null;
		file = new File(filePath);
		FileOutputStream fos = null;
		if (!file.exists()) {
			file.createNewFile();
		}
		fos = new FileOutputStream(file);
		fos.write(text.getBytes());

		fos.close();
	}

	private void destroyRConnection(Tuple2 <RConnection, Integer> runner) {
		try {
			runner.f0.eval("library(RSclient)");
			System.out.println(String.format("rsc <- RSconnect(port = %s)", runner.f1));
			rEval(runner.f0, String.format("eval(rsc <- RSconnect(port = %s))", runner.f1));
			//runner.f0.eval(String.format("rsc <- RSconnect(port = %s)", runner.f1));
			runner.f0.eval("RSshutdown(rsc)");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		runner.f0.close();
	}

	@Override
	public void close() {
		this.runner.close();
	}

	private static REXP mTableToRDF(MTable mt) throws Exception {
		String[] colNames = mt.getColNames();
		TypeInformation <?>[] colTypes = mt.getColTypes();
		List <Row> rows = mt.getRows();

		int rowNum = mt.getNumRow();
		int colNum = mt.getNumCol();

		RList rListS = new RList();

		for (int j = 0; j < colNum; j++) {
			if (colTypes[j] == AlinkTypes.INT) {
				int[] tmps = new int[rowNum];
				for (int i = 0; i < rowNum; i++) {
					tmps[i] = (int) rows.get(i).getField(j);
				}
				rListS.add(new REXPInteger(tmps));
			} else if (colTypes[j] == AlinkTypes.STRING) {
				String[] tmps = new String[rowNum];
				for (int i = 0; i < rowNum; i++) {
					tmps[i] = (String) rows.get(i).getField(j);
				}
				rListS.add(new REXPString(tmps));
			} else if (colTypes[j] == AlinkTypes.DOUBLE) {
				double[] tmps = new double[rowNum];
				for (int i = 0; i < rowNum; i++) {
					tmps[i] = (double) rows.get(i).getField(j);
				}
				rListS.add(new REXPDouble(tmps));
			}
		}

		//add names
		for (int i = 0; i < colNames.length; i++) {
			rListS.setKeyAt(i, colNames[i]);
		}

		return REXP.createDataFrame(rListS);
	}

	private static MTable[] rDfToMTables(REXP rt, String[] outCols) throws Exception {
		if (1 == outCols.length) {
			return new MTable[] {rDfToMTable(rt)};
		} else {
			MTable[] outMT = new MTable[outCols.length];
			RList rList = rt.asList();
			if (outCols.length != rList.size()) {
				throw new RuntimeException("r out size is not equal with output cols");
			}
			for (int i = 0; i < outMT.length; i++) {
				outMT[i] = rDfToMTable((REXP) rList.get(i));
			}
			return outMT;
		}
	}

	private static MTable rDfToMTable(REXP rt) throws Exception {
		if (rt.isList()) {
			RList rList = rt.asList();

			String[] keys = rList.keys();
			List <Row> rowRe = new ArrayList <>();
			int rowNum = ((REXP) rList.get(0)).asStrings().length;
			int colNum = keys.length;
			TypeInformation <?>[] mTableTypes = new TypeInformation[colNum];
			for (int i = 0; i < rowNum; i++) {
				rowRe.add(new Row(colNum));
			}

			for (int colIdx = 0; colIdx < keys.length; colIdx++) {
				REXP ex = (REXP) rList.get(keys[colIdx]);
				if (ex.isInteger()) {
					int[] tmps = ex.asIntegers();
					for (int i = 0; i < tmps.length; i++) {
						rowRe.get(i).setField(colIdx, tmps[i]);
					}
					mTableTypes[colIdx] = AlinkTypes.INT;
				} else if (ex.isNumeric()) {
					double[] tmps = ex.asDoubles();
					for (int i = 0; i < tmps.length; i++) {
						rowRe.get(i).setField(colIdx, tmps[i]);
					}
					mTableTypes[colIdx] = AlinkTypes.DOUBLE;
				} else if (ex.isString()) {
					String[] tmps = ex.asStrings();
					for (int i = 0; i < tmps.length; i++) {
						rowRe.get(i).setField(colIdx, tmps[i]);
					}
					mTableTypes[colIdx] = AlinkTypes.STRING;
				}
			}
			return new MTable(rowRe, keys, mTableTypes);
		} else if (rt.isString()) {
			return new MTable(
				Collections.singletonList(Row.of(JsonConverter.toJson(rt.asStrings()))),
				new String[] {"col_str"},
				new TypeInformation <?>[] {AlinkTypes.STRING});
		} else if (rt.isNumeric()) {
			return new MTable(
				Collections.singletonList(Row.of(JsonConverter.toJson(rt.asStrings()))),
				new String[] {"col_str"},
				new TypeInformation <?>[] {AlinkTypes.STRING});
		} else {
			throw new RuntimeException("REXP type not support." + rt.getClass());
		}
	}

	static class RserveCallable implements Callable <Integer> {
		private int port;
		private String pluginPath;
		private Process pro;
		private int status; //-1 is fail, 0 is ok, 1 need wait.

		public RserveCallable(String pluginPath, int port) {
			this.port = port;
			this.pluginPath = pluginPath;
			this.status = 1;
		}

		@Override
		public Integer call() throws Exception {
			this.pro = null;
			OsType systemType = OsUtils.getSystemType();
			String rBin = null;
			String rserveBin = null;
			switch (systemType) {
				case MACOSX:
					rBin = pluginPath + "/R";
					rserveBin = pluginPath + "/library/Rserve/libs/Rserve";
					break;
				case LINUX:
					rBin = pluginPath + "/lib64/R/bin/R";
					rserveBin = pluginPath + "/lib64/R/library/Rserve/libs//Rserve";
					break;
				default:
					throw new RuntimeException("It is not support. " + systemType.name());
			}
			try {

				List <String> args = Arrays.asList(
					rBin, "CMD", rserveBin,
					"--no-save",
					"--RS-port", Integer.toString(port)
				);

				ProcessBuilder pb = new ProcessBuilder(args);
				System.out.println("the command is: " + String.join(" ", pb.command()));
				pro = pb.start();

				BufferedReader br = new BufferedReader(new InputStreamReader(pro.getInputStream()), 4096);
				String line = null;
				int i = 0;

				StringBuilder sb = new StringBuilder();
				while ((line = br.readLine()) != null) {
					if (0 != i) {sb.append("\r\n");}
					i++;
					sb.append(line);
					if (line.equals("Rserv started in daemon mode.")) {
						this.status = 0;
					} else if (line.contains("address already in use")) {
						this.status = -1;
						return 0;
					}

				}
				System.out.println(sb.toString());

				BufferedReader brE = new BufferedReader(new InputStreamReader(pro.getErrorStream()), 4096);
				String lineE = null;
				int iE = 0;

				StringBuilder sbE = new StringBuilder();
				while ((lineE = brE.readLine()) != null) {
					if (0 != iE) {sbE.append("\r\n");}
					iE++;
					sbE.append(lineE);
					if (line.equals("Rserv started in daemon mode.")) {
						this.status = 0;
					} else if (line.contains("address already in use")) {
						this.status = -1;
						return 0;
					}
				}
				System.err.println(sbE.toString());

				br.close();
				brE.close();
			} catch (Exception ex) {
				ex.printStackTrace();
				pro.destroy();
			}

			return 0;
		}

		public void destroy() {
			if (null != this.pro) {
				this.pro.destroy();
			}
		}

	}

}


