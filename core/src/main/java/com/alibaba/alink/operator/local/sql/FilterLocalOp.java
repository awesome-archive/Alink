package com.alibaba.alink.operator.local.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.LocalMLEnvironment;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.sql.FilterParams;

import java.util.ArrayList;
import java.util.List;

/**
 * Filter records in the batch operator.
 */
@NameCn("SQL操作：Filter")
public final class FilterLocalOp extends BaseSqlApiLocalOp <FilterLocalOp>
	implements FilterParams <FilterLocalOp> {

	private static final long serialVersionUID = 1182682104232353734L;

	public FilterLocalOp() {
		this(new Params());
	}

	public FilterLocalOp(String clause) {
		this(new Params().set(CLAUSE, clause));
	}

	public FilterLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		String predicate = getClause();
		LocalOperator <?> in = inputs[0];

		try {
			if (predicate.contains(">=")) {
				String[] splits = predicate.split(">=");
				if (splits.length == 2) {
					String colName = splits[0].trim();
					if (colName.contains("`")) {
						colName = colName.replace("`", "");
					}
					double comVal = Double.parseDouble(splits[1].trim());
					int colIdx = checkCol(in.getSchema(), colName);

					if (colIdx >= 0) {
						List <Row> rows = in.collect();
						List <Row> outRows = new ArrayList <>();
						for (Row row : rows) {
							Object val = row.getField(colIdx);
							if (val != null && ((Number) val).doubleValue() >= comVal) {
								outRows.add(row);
							}
						}
						this.setOutputTable(new MTable(outRows, in.getSchema()));
						return;
					} else {
						throw new AkIllegalStateException("col is not exsit.");
					}
				}
			} else if (predicate.contains("=")) {
				String[] splits = predicate.split("=");
				if (splits.length == 2) {
					String colName = splits[0].trim();
					if (colName.contains("`")) {
						colName = colName.replace("`", "");
					}
					String comVal = splits[1];

					int colIdx = checkCol(in.getSchema(), colName);

					comVal = comVal.trim();
					if (TableUtil.isString(TableUtil.findColType(in.getSchema(), colName))) {
						checkStringVal(comVal);
						comVal = comVal.substring(1, comVal.length() - 1);
					} else {
						checkOtherVal(comVal);
					}

					List <Row> rows = inputs[0].collect();
					List <Row> outRows = new ArrayList <>();
					for (Row row : rows) {
						Object val = row.getField(colIdx);
						if (val != null && String.valueOf(val).equals(comVal)) {
							outRows.add(row);
						}
					}
					this.setOutputTable(new MTable(outRows, inputs[0].getSchema()));
					return;
				}
			}
		} catch (Exception ex) {
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(ex.getMessage());
			}
		}

		LocalOperator <?> outOp = in.select(String.format("*, %s as filter_col", predicate))
			.filter("filter_col=true")
			.select(in.getColNames());

		this.setOutputTable(outOp.getOutputTable());
	}

	private static int checkCol(TableSchema schema, String colName) {
		int colIdx = TableUtil.findColIndex(schema, colName);
		if (colIdx < 0) {
			throw new AkIllegalStateException("col is not exsit.");
		}

		return colIdx;

	}

	private static void checkStringVal(String comVal) {
		if (comVal.startsWith("'") && comVal.endsWith("'")) {
		} else {
			throw new AkIllegalStateException("val is not value.");
		}
	}

	private static void checkOtherVal(String comVal) {
		if (comVal.contains("(") ||
			comVal.contains(")") ||
			comVal.contains("'")
		) {
			throw new AkIllegalStateException("val is not value.");
		}
	}
}
