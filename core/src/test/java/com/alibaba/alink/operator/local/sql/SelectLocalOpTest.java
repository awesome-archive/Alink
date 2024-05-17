package com.alibaba.alink.operator.local.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.sql.Select;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SelectLocalOpTest {

	@Test
	public void testPointInColName() {
		Row[] array = new Row[] {
			Row.of(VectorUtil.getVector("$31$0:1.0 1:1.0 2:1.0 30:1.0"), "1.0  1.0  1.0  1.0", 1.0, 1.0, 1.0, 1.0, 1),
			Row.of(VectorUtil.getVector("$31$0:1.0 1:1.0 2:0.0 30:1.0"), "1.0  1.0  0.0  1.0", 1.0, 1.0, 0.0, 1.0, 1),
			Row.of(VectorUtil.getVector("$31$0:1.0 1:0.0 2:1.0 30:1.0"), "1.0  0.0  1.0  1.0", 1.0, 0.0, 1.0, 1.0, 1),
			Row.of(VectorUtil.getVector("$31$0:1.0 1:0.0 2:1.0 30:1.0"), "1.0  0.0  1.0  1.0", 1.0, 0.0, 1.0, 1.0, 1),
			Row.of(VectorUtil.getVector("$31$0:0.0 1:1.0 2:1.0 30:0.0"), "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of(VectorUtil.getVector("$31$0:0.0 1:1.0 2:1.0 30:0.0"), "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of(VectorUtil.getVector("$31$0:0.0 1:1.0 2:1.0 30:0.0"), "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of(VectorUtil.getVector("$31$0:0.0 1:1.0 2:1.0 30:0.0"), "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0)
		};

		LocalOperator <?> source = new MemSourceLocalOp(
			Arrays.asList(array),
			new TableSchema(
				new String[] {"svec", "vec", "f0", "a.f1", "f2", "f3", "labels"},
				new TypeInformation <?>[] {
					AlinkTypes.SPARSE_VECTOR,
					AlinkTypes.DENSE_VECTOR,
					AlinkTypes.DOUBLE,
					AlinkTypes.DOUBLE,
					AlinkTypes.DOUBLE,
					AlinkTypes.DOUBLE,
					AlinkTypes.DOUBLE,
				}));

		source.select("a.f1, (a.f1+1) as c1").print();

	}

	@Test
	public void testVector() {
		Row[] array = new Row[] {
			Row.of(VectorUtil.getVector("$31$0:1.0 1:1.0 2:1.0 30:1.0"), "1.0  1.0  1.0  1.0", 1.0, 1.0, 1.0, 1.0, 1),
			Row.of(VectorUtil.getVector("$31$0:1.0 1:1.0 2:0.0 30:1.0"), "1.0  1.0  0.0  1.0", 1.0, 1.0, 0.0, 1.0, 1),
			Row.of(VectorUtil.getVector("$31$0:1.0 1:0.0 2:1.0 30:1.0"), "1.0  0.0  1.0  1.0", 1.0, 0.0, 1.0, 1.0, 1),
			Row.of(VectorUtil.getVector("$31$0:1.0 1:0.0 2:1.0 30:1.0"), "1.0  0.0  1.0  1.0", 1.0, 0.0, 1.0, 1.0, 1),
			Row.of(VectorUtil.getVector("$31$0:0.0 1:1.0 2:1.0 30:0.0"), "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of(VectorUtil.getVector("$31$0:0.0 1:1.0 2:1.0 30:0.0"), "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of(VectorUtil.getVector("$31$0:0.0 1:1.0 2:1.0 30:0.0"), "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of(VectorUtil.getVector("$31$0:0.0 1:1.0 2:1.0 30:0.0"), "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0)
		};

		LocalOperator <?> source = new MemSourceLocalOp(
			Arrays.asList(array),
			new TableSchema(
				new String[] {"svec", "vec", "f0", "f1", "f2", "f3", "labels"},
				new TypeInformation <?>[] {
					AlinkTypes.SPARSE_VECTOR,
					AlinkTypes.DENSE_VECTOR,
					AlinkTypes.DOUBLE,
					AlinkTypes.DOUBLE,
					AlinkTypes.DOUBLE,
					AlinkTypes.DOUBLE,
					AlinkTypes.DOUBLE,
				}));

		source.select("svec, f0, 'neg' as f11")
			.print("********** simple ********");
		source.select("svec, f0, (f1 + cast(0.5 as double)) as f11")
			.print("********** test  ********");
		source.select("CAST(f0 AS VARCHAR) AS f0_str,  CAST(f1 as VARCHAR) AS f1_str, svec, vec, f0, f1")
			.select("f0,f1,concat(f0_str, ',') as f0_f1")
			.print("********** test for , ********");

	}

	@Test
	public void testPipeline() {
		Row[] array = new Row[] {
			Row.of(VectorUtil.getVector("$31$0:1.0 1:1.0 2:1.0 30:1.0"), "1.0  1.0  1.0  1.0", 1.0, 1.0, 1.0, 1.0, 1),
			Row.of(VectorUtil.getVector("$31$0:1.0 1:1.0 2:0.0 30:1.0"), "1.0  1.0  0.0  1.0", 1.0, 1.0, 0.0, 1.0, 1),
			Row.of(VectorUtil.getVector("$31$0:1.0 1:0.0 2:1.0 30:1.0"), "1.0  0.0  1.0  1.0", 1.0, 0.0, 1.0, 1.0, 1),
			Row.of(VectorUtil.getVector("$31$0:1.0 1:0.0 2:1.0 30:1.0"), "1.0  0.0  1.0  1.0", 1.0, 0.0, 1.0, 1.0, 1),
			Row.of(VectorUtil.getVector("$31$0:0.0 1:1.0 2:1.0 30:0.0"), "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of(VectorUtil.getVector("$31$0:0.0 1:1.0 2:1.0 30:0.0"), "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of(VectorUtil.getVector("$31$0:0.0 1:1.0 2:1.0 30:0.0"), "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of(VectorUtil.getVector("$31$0:0.0 1:1.0 2:1.0 30:0.0"), "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0)
		};

		LocalOperator <?> source = new MemSourceLocalOp(
			Arrays.asList(array),
			new TableSchema(
				new String[] {"svec", "vec", "f0", "f1", "f2", "f3", "labels"},
				new TypeInformation <?>[] {
					AlinkTypes.SPARSE_VECTOR,
					AlinkTypes.DENSE_VECTOR,
					AlinkTypes.DOUBLE,
					AlinkTypes.DOUBLE,
					AlinkTypes.DOUBLE,
					AlinkTypes.DOUBLE,
					AlinkTypes.DOUBLE,
				}));

		Pipeline pipeline = new Pipeline()
			.add(new Select().setClause("svec, f0, 'neg' as f11"));

		pipeline.fit(source).transform(source).print();

		Pipeline pipeline2 = new Pipeline()
			.add(new Select().setClause("svec, f0, (f1 + cast(0.5 as double)) as f11"));

		pipeline2.fit(source).transform(source).print();

		Pipeline pipeline3 = new Pipeline()
			.add(new Select().setClause(
				"CAST(f0 AS VARCHAR) AS f0_str,  CAST(f1 as VARCHAR) AS f1_str, svec, vec, f0, f1"))
			.add(new Select().setClause("f0,f1,concat(f0_str, ',') as f0_f1"))
			;

		pipeline3.fit(source).transform(source).print();
	}

	@Test
	public void testSelectLocalOp() {
		//String URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv";
		//String SCHEMA_STR
		//	= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
		//LocalOperator <?> data = new TableSourceLocalOp(
		//	new CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR).collectMTable());
		LocalOperator <?> data = IrisData.getLocalSourceOp();
		data.link(new SelectLocalOp().setClause("category as a")).print();
	}

	@Test
	public void testSimpleSelect() throws Exception {
		data().link(
			new SelectLocalOp()
				.setClause("f_double, f_long")
		).print();
	}

	@Test
	public void testSimpleSelect2() throws Exception {
		data().select("f_double, f_long").print();
	}

	@Test
	public void testCSelect() throws Exception {
		data().link(
			new SelectLocalOp()
				.setClause("f_double,`f_l.*`, f_double+1 as f_double_1")
		).print();
	}

	@Test
	public void testCSelect2() throws Exception {
		data().select("f_double, `f_l.*`,f_double+1 as f_double_1").print();
	}

	@Test
	public void testCSelect3() throws Exception {
		data().select("f_double as fr, *, f_long As fr2").print();
	}

	@Test
	public void testCSelect4() throws Exception {
		data()
			.select("f_string as fas, f_double, f_long")
			.select("fas, f_double")
			.select("fas as as2, f_double as fd, f_double")
			.print();
	}

	@Test
	public void testCSelect5() throws Exception {
		String[] originSqlCols = data().select("f_string, f_double, f_string, f_string").getColNames();
		String[] simpleSelectCols = data().select("f_string, f_double, f_string, f_string").getColNames();
		Assert.assertArrayEquals(originSqlCols, simpleSelectCols);
	}

	private LocalOperator <?> data() {
		List <Row> testArray = Arrays.asList(
			Row.of("a", 1L, 1, 2.0, true),
			Row.of(null, 2L, 2, -3.0, true),
			Row.of("c", null, null, 2.0, false),
			Row.of("a", 0L, 0, null, null)
		);

		// for test
		String[] colNames = new String[] {"f_string", "f_long", "f_lint", "f_double", "f_boolean"};

		return new MemSourceLocalOp(testArray, colNames);
	}

}