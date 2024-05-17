package com.alibaba.alink.operator.common.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.common.exceptions.AkParseErrorException;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.linalg.Tensor;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.sql.builtin.string.string.DateAdd;
import com.alibaba.alink.common.sql.builtin.string.string.DateDiff;
import com.alibaba.alink.common.sql.builtin.string.string.DateSub;
import com.alibaba.alink.common.sql.builtin.string.string.KeyValue;
import com.alibaba.alink.common.sql.builtin.string.string.RegExp;
import com.alibaba.alink.common.sql.builtin.string.string.RegExpExtract;
import com.alibaba.alink.common.sql.builtin.string.string.RegExpReplace;
import com.alibaba.alink.common.sql.builtin.string.string.SplitPart;
import com.alibaba.alink.common.sql.builtin.time.DataFormat;
import com.alibaba.alink.common.sql.builtin.time.FromUnixTime;
import com.alibaba.alink.common.sql.builtin.time.Now;
import com.alibaba.alink.common.sql.builtin.time.ToTimeStamp;
import com.alibaba.alink.common.sql.builtin.time.UnixTimeStamp;
import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.BatchSqlOperators;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.common.sql.functions.MathFunctions;
import com.alibaba.alink.operator.common.sql.functions.StringFunctions;
import com.alibaba.alink.params.sql.SelectParams;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.util.BuiltInMethod;
import scala.Int;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * Execute the sql select operation without Flink.
 */
public class CalciteSelectMapper extends Mapper {

	private static final long serialVersionUID = 6207092249511500058L;

	private final ConcurrentHashMap <Thread, Connection> threadConnectionMap = new ConcurrentHashMap <>();
	private final ConcurrentHashMap <Thread, PreparedStatement> threadPreparedStatementMap =
		new ConcurrentHashMap <>();

	private final static String TEMPLATE = "SELECT %s FROM (SELECT %s FROM (VALUES (1))) foo";

	private final static String COL_NAME_PREFIX = "alink_prefix_";

	/**
	 * Constructor.
	 *
	 * @param dataSchema input table schema.
	 * @param params     input parameters.
	 */
	public CalciteSelectMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	// Case sensitive.
	public static void registerFlinkBuiltInFunctions(SchemaPlus schema) {
		BiConsumer <String, Method> addScalarFunctionConsumer =
			(k, v) -> schema.add(k, ScalarFunctionImpl.create(v));

		addScalarFunctionConsumer.accept("LOG2", MathFunctions.LOG2);
		addScalarFunctionConsumer.accept("LOG2", MathFunctions.LOG2_DEC);
		addScalarFunctionConsumer.accept("LOG", MathFunctions.LOG);
		addScalarFunctionConsumer.accept("LOG", MathFunctions.LOG_DEC);
		addScalarFunctionConsumer.accept("LOG", MathFunctions.LOG_WITH_BASE);
		addScalarFunctionConsumer.accept("LOG", MathFunctions.LOG_WITH_BASE_DEC_DOU);
		addScalarFunctionConsumer.accept("LOG", MathFunctions.LOG_WITH_BASE_DOU_DEC);
		addScalarFunctionConsumer.accept("LOG", MathFunctions.LOG_WITH_BASE_DEC_DEC);
		addScalarFunctionConsumer.accept("SINH", MathFunctions.SINH);
		addScalarFunctionConsumer.accept("SINH", MathFunctions.SINH_DEC);
		addScalarFunctionConsumer.accept("COSH", MathFunctions.COSH);
		addScalarFunctionConsumer.accept("COSH", MathFunctions.COSH_DEC);
		addScalarFunctionConsumer.accept("TANH", MathFunctions.TANH);
		addScalarFunctionConsumer.accept("TANH", MathFunctions.TANH_DEC);
		addScalarFunctionConsumer.accept("UUID", MathFunctions.UUID);
		addScalarFunctionConsumer.accept("BIN", MathFunctions.BIN);
		addScalarFunctionConsumer.accept("HEX", MathFunctions.HEX_LONG);
		addScalarFunctionConsumer.accept("HEX", MathFunctions.HEX_STRING);

		addScalarFunctionConsumer.accept("FROM_BASE64", StringFunctions.FROMBASE64);
		addScalarFunctionConsumer.accept("TO_BASE64", StringFunctions.TOBASE64);
		addScalarFunctionConsumer.accept("LPAD", StringFunctions.LPAD);
		addScalarFunctionConsumer.accept("RPAD", StringFunctions.RPAD);

		addScalarFunctionConsumer.accept("REGEXP_EXTRACT", StringFunctions.REGEXP_EXTRACT);
		addScalarFunctionConsumer.accept("CONCAT", StringFunctions.CONCAT);
		addScalarFunctionConsumer.accept("CONCAT", StringFunctions.CONCAT3);
		addScalarFunctionConsumer.accept("CONCAT", StringFunctions.CONCAT4);
		addScalarFunctionConsumer.accept("CONCAT", StringFunctions.CONCAT5);

		addScalarFunctionConsumer.accept("LTRIM", BuiltInMethod.LTRIM.method);
		addScalarFunctionConsumer.accept("RTRIM", BuiltInMethod.RTRIM.method);

		addScalarFunctionConsumer.accept("MD5", StringFunctions.MD5);
		addScalarFunctionConsumer.accept("SHA1", StringFunctions.SHA1);
		addScalarFunctionConsumer.accept("SHA224", StringFunctions.SHA224);
		addScalarFunctionConsumer.accept("SHA256", StringFunctions.SHA256);
		addScalarFunctionConsumer.accept("SHA384", StringFunctions.SHA384);
		addScalarFunctionConsumer.accept("SHA512", StringFunctions.SHA512);
		addScalarFunctionConsumer.accept("SHA2", StringFunctions.SHA2);

		// time function.
		addScalarFunctionConsumer.accept("NOW", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			Now.class, "eval", int.class));
		addScalarFunctionConsumer.accept("NOW", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			Now.class, "eval"));

		addScalarFunctionConsumer.accept("DATE_FORMAT_LTZ", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			DataFormat.class, "eval", Timestamp.class));
		addScalarFunctionConsumer.accept("DATE_FORMAT_LTZ", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			DataFormat.class, "eval", Timestamp.class, String.class));

		addScalarFunctionConsumer.accept("TO_TIMESTAMP", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			ToTimeStamp.class, "eval", Long.class));
		addScalarFunctionConsumer.accept("TO_TIMESTAMP", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			ToTimeStamp.class, "eval", Integer.class));
		addScalarFunctionConsumer.accept("TO_TIMESTAMP", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			ToTimeStamp.class, "eval", String.class));
		addScalarFunctionConsumer.accept("TO_TIMESTAMP", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			ToTimeStamp.class, "eval", String.class, String.class));

		addScalarFunctionConsumer.accept("UNIX_TIMESTAMP", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			UnixTimeStamp.class, "eval"));
		addScalarFunctionConsumer.accept("UNIX_TIMESTAMP", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			UnixTimeStamp.class, "eval", String.class));
		addScalarFunctionConsumer.accept("UNIX_TIMESTAMP", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			UnixTimeStamp.class, "eval", Timestamp.class));
		addScalarFunctionConsumer.accept("UNIX_TIMESTAMP", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			UnixTimeStamp.class, "eval", String.class, String.class));

		addScalarFunctionConsumer.accept("FROM_UNIXTIME", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			FromUnixTime.class, "eval", Long.class));
		addScalarFunctionConsumer.accept("FROM_UNIXTIME", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			FromUnixTime.class, "eval", Integer.class));
		addScalarFunctionConsumer.accept("FROM_UNIXTIME", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			FromUnixTime.class, "eval", Long.class, String.class));
		addScalarFunctionConsumer.accept("FROM_UNIXTIME", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			FromUnixTime.class, "eval", Integer.class, String.class));

		// for other
		addScalarFunctionConsumer.accept("SPLIT_PART", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			SplitPart.class, "eval", String.class, String.class, Integer.class));
		addScalarFunctionConsumer.accept("SPLIT_PART", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			SplitPart.class, "eval", String.class, String.class, Integer.class, Integer.class));

		addScalarFunctionConsumer.accept("KEYVALUE", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			KeyValue.class, "eval", String.class, String.class, String.class, String.class));

		addScalarFunctionConsumer.accept("DATEDIFF", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			DateDiff.class, "eval", String.class, Timestamp.class));
		addScalarFunctionConsumer.accept("DATEDIFF", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			DateDiff.class, "eval", String.class, String.class));
		addScalarFunctionConsumer.accept("DATEDIFF", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			DateDiff.class, "eval", Timestamp.class, Timestamp.class));
		addScalarFunctionConsumer.accept("DATEDIFF", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			DateDiff.class, "eval", Timestamp.class, String.class));

		addScalarFunctionConsumer.accept("REGEXP", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			RegExp.class, "eval", String.class, String.class));

		addScalarFunctionConsumer.accept("REGEXP_EXTRACT", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			RegExpExtract.class, "eval", String.class, String.class, int.class));

		addScalarFunctionConsumer.accept("DATE_ADD", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			DateAdd.class, "eval", String.class, int.class));
		addScalarFunctionConsumer.accept("DATE_ADD", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			DateAdd.class, "eval", Timestamp.class, int.class));

		addScalarFunctionConsumer.accept("DATE_SUB", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			DateSub.class, "eval", String.class, int.class));
		addScalarFunctionConsumer.accept("DATE_SUB", org.apache.calcite.linq4j.tree.Types.lookupMethod(
			DateSub.class, "eval", Timestamp.class, int.class));

		if (AlinkGlobalConfiguration.getFlinkVersion().equals("flink-1.9") ||
			AlinkGlobalConfiguration.getFlinkVersion().equals("flink-1.10") ||
			AlinkGlobalConfiguration.getFlinkVersion().equals("flink-1.11")) {
			addScalarFunctionConsumer.accept("REGEXP_REPLACE", StringFunctions.REGEXP_REPLACE);
			addScalarFunctionConsumer.accept("REGEXP_REPLACE", org.apache.calcite.linq4j.tree.Types.lookupMethod(
				RegExpReplace.class, "eval", String.class, String.class, String.class));
		}
	}

	@Override
	public void close() {
		super.close();
		try {
			for (PreparedStatement preparedStatement : threadPreparedStatementMap.values()) {
				preparedStatement.close();
			}
			for (Connection connection : threadConnectionMap.values()) {
				connection.close();
			}
		} catch (SQLException e) {
			throw new AkUnclassifiedErrorException("Failed to close prepared statement or connection.", e);
		}
	}

	private Connection getConnection() {
		/*
		In EAS, threads started have no contextClassLoader, which makes {@link
		CompilerFactoryFactory#getDefaultCompilerFactory} failed. So we manually set it to the classloader of this
		class.
		 */

		Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
		try {
			Properties info = new Properties();
			info.setProperty(CalciteConnectionProperty.DEFAULT_NULL_COLLATION.camelName(), NullCollation.LAST.name());
			info.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
			info.setProperty(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.name());
			info.setProperty(CalciteConnectionProperty.QUOTING.camelName(), Quoting.BACK_TICK.name());
			Class.forName("org.apache.calcite.jdbc.Driver");

			return DriverManager.getConnection("jdbc:calcite:fun=mysql", info);
		} catch (ClassNotFoundException | SQLException e) {
			throw new AkUnclassifiedErrorException("Failed to initialize JDBC connection.", e);
		}
	}

	private PreparedStatement getPreparedStatement() {
		Connection connection = threadConnectionMap.computeIfAbsent(Thread.currentThread(), d -> getConnection());
		CalciteConnection calciteConnection;
		try {
			calciteConnection = connection.unwrap(CalciteConnection.class);
		} catch (SQLException e) {
			throw new AkIllegalStateException("Failed to unwrap CalciteConnection instance.", e);
		}

		SchemaPlus rootSchema = calciteConnection.getRootSchema();
		registerFlinkBuiltInFunctions(rootSchema);

		TableSchema dataSchema = getDataSchema();
		String clause = params.get(SelectParams.CLAUSE);

		TypeInformation <?>[] fieldTypes = dataSchema.getFieldTypes();
		String[] fieldNames = dataSchema.getFieldNames();
		int fieldCount = fieldNames.length;

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < fieldCount; i += 1) {
			if (i > 0) {
				sb.append(", ");
			}
			sb.append("CAST(? as ");
			sb.append(getTypeString(fieldTypes[i]));
			sb.append(") as ");
			sb.append("`" + fieldNames[i] + "`");
		}

		String query = String.format(TEMPLATE, clause, sb);

		try {
			return calciteConnection.prepareStatement(query);
		} catch (SQLException e) {
			throw new AkParseErrorException(String.format("Failed to prepare query statement: %s", query), e);
		}
	}

	private String getTypeString(TypeInformation <?> dataType) {
		if (AlinkTypes.isVectorType(dataType)
			|| AlinkTypes.isMTableType(dataType)
			|| AlinkTypes.isTensorType(dataType)) {
			return "DOUBLE";
		}
		return FlinkTypeConverter.getTypeString(dataType);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		PreparedStatement preparedStatement = threadPreparedStatementMap.computeIfAbsent(
			Thread.currentThread(), d -> getPreparedStatement());

		for (int i = 0; i < selection.length(); i += 1) {
			Object v = selection.get(i);
			if (v instanceof BigDecimal) {
				preparedStatement.setObject(i + 1, v, java.sql.Types.DECIMAL);
			} else if (v instanceof BigInteger) {
				preparedStatement.setObject(i + 1, v, java.sql.Types.BIGINT);
			} else if (v instanceof Vector || v instanceof MTable || v instanceof Tensor) {
				preparedStatement.setObject(i + 1, null, java.sql.Types.DOUBLE);
			} else if (v instanceof Float) {
				preparedStatement.setObject(i + 1, v, java.sql.Types.FLOAT);
			} else if (v instanceof Integer) {
				preparedStatement.setObject(i + 1, v, Types.INTEGER);
			}
			//else if (v instanceof Timestamp) {
			//	preparedStatement.setObject(i + 1, v, Types.TIMESTAMP);
			//}
			else {
				preparedStatement.setObject(i + 1, v);
			}
		}
		try (ResultSet resultSet = preparedStatement.executeQuery()) {
			ResultSetMetaData metaData = resultSet.getMetaData();
			int columnCount = metaData.getColumnCount();
			if (resultSet.next()) {
				for (int i = 0; i < columnCount; i += 1) {
					result.set(i, resultSet.getObject(i + 1));
				}
			}
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema dataSchema,
																						   Params params) {
		return prepareIoSchemaImpl(dataSchema, params);
	}

	static Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchemaImpl(TableSchema dataSchema,
																							Params params) {
		String clause = params.get(SelectParams.CLAUSE);
		Long newMLEnvId = MLEnvironmentFactory.getNewMLEnvironmentId();

		String[] colNames = dataSchema.getFieldNames();
		String[] newColNames = new String[colNames.length];
		for (int i = 0; i < colNames.length; i++) {
			newColNames[i] = colNames[i].replace(".", "_alink_point_");
		}

		TypeInformation <?>[] colTypes = dataSchema.getFieldTypes();
		TypeInformation <?>[] newColTypes = new TypeInformation[colTypes.length];
		for (int i = 0; i < colTypes.length; i++) {
			if (AlinkTypes.isVectorType(colTypes[i])
				|| AlinkTypes.isMTableType(colTypes[i])
				|| AlinkTypes.isTensorType(colTypes[i])) {
				newColTypes[i] = AlinkTypes.DOUBLE;
			} else {
				newColTypes[i] = colTypes[i];
			}
		}

		MemSourceBatchOp source = new MemSourceBatchOp(Collections.emptyList(),
			new TableSchema(newColNames, newColTypes))
			.setMLEnvironmentId(newMLEnvId);

		String newClause = clause;
		for (int i = 0; i < colNames.length; i++) {
			newClause.replaceAll(colNames[i], newColNames[i]);
		}

		newClause = SelectUtils.convertRegexClause2ColNames(newColNames, newClause);

		TableSchema outputSchema = BatchSqlOperators.select(source, newClause).getOutputTable().getSchema();

		MLEnvironmentFactory.remove(newMLEnvId);

		return Tuple4.of(
			dataSchema.getFieldNames(),
			outputSchema.getFieldNames(),
			outputSchema.getFieldTypes(),
			new String[0]
		);
	}
}
