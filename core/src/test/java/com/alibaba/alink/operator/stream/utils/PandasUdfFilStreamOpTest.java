package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.OverCountWindowStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class PandasUdfFilStreamOpTest {
	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	private String scriptFile;

	@Before
	public void before() throws Exception {
		File zipFile = folder.newFile("PandasUdfFilStreamOpTest.zip");

		try (ZipOutputStream zipOutputStream = new ZipOutputStream(Files.newOutputStream(zipFile.toPath()))) {
			ZipEntry zipEntry = new ZipEntry("__init__.py");
			zipOutputStream.putNextEntry(zipEntry);
			zipOutputStream.closeEntry();
			ZipEntry zipScriptEntry = new ZipEntry("alink_stream_test.py");
			zipOutputStream.putNextEntry(zipScriptEntry);
			String script = "# -*- coding: utf-8 -*-\n\ndef processDf(dfGroupData, dfGroupData1):\n"
				+ "    return [dfGroupData, dfGroupData1]\n";
			zipOutputStream.write(script.getBytes(StandardCharsets.UTF_8));
			zipOutputStream.closeEntry();
		}

		scriptFile = zipFile.getAbsolutePath();
	}

	@Test
	@Ignore
	public void testFile() throws Exception {
		List <Row> mTableData = Arrays.asList(
			Row.of(1, new Timestamp(1), 10.0),
			Row.of(1, new Timestamp(2), 11.0),
			Row.of(1, new Timestamp(3), 12.0),
			Row.of(1, new Timestamp(4), 13.0),
			Row.of(1, new Timestamp(5), 14.0),
			Row.of(1, new Timestamp(6), 15.0),
			Row.of(1, new Timestamp(7), 16.0),
			Row.of(1, new Timestamp(8), 17.0),
			Row.of(1, new Timestamp(9), 18.0),
			Row.of(1, new Timestamp(10), 19.0)
		);

		MemSourceStreamOp source = new MemSourceStreamOp(mTableData, new String[] {"id", "ts", "val"});

		OverCountWindowStreamOp overCountWindowStreamOp = new OverCountWindowStreamOp()
			.setGroupCols("id")
			.setTimeCol("ts")
			.setPrecedingRows(5)
			.setClause("mtable_agg_preceding(ts, val) as group_data, mtable_agg_preceding(ts, val) as group_data1");

		PandasUdfFilStreamOp pandasUdf = new PandasUdfFilStreamOp()
			.setSelectedCols("group_data", "group_data1")
			.setOutputCols("grouped_data", "grouped1_data")
			.setUserFilePaths(scriptFile)
			.setFunctionParams("{\"a\": \"b\"}")
			.setClassName("alink_stream_test.processDf");

		source.link(overCountWindowStreamOp).link(pandasUdf).print();

		StreamOperator.execute();
	}
}
