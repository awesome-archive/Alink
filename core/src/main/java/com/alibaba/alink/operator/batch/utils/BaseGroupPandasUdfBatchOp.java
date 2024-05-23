package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.mapper.MapperAdapterMT;
import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.PandasUdfMapper;
import com.alibaba.alink.params.timeseries.PandasUdfParams;
import com.alibaba.alink.params.udf.BaseGroupPandasUdfParams;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Transform MTable format recommendation to table format.
 */
@Internal
abstract class BaseGroupPandasUdfBatchOp<T extends BaseGroupPandasUdfBatchOp <T>> extends BatchOperator <T>
	implements BaseGroupPandasUdfParams <T> {

	private static final Logger LOG = LoggerFactory.getLogger(BaseGroupPandasUdfBatchOp.class);

	private static final long serialVersionUID = 790348573681664909L;

	private final static String MTABLE_COL_NAME = "grouped_pandas_udf_col_mtable";

	public BaseGroupPandasUdfBatchOp() {
		this(null);
	}

	public BaseGroupPandasUdfBatchOp(Params params) {
		super(params);
	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = inputs[0];

		String[] groupCols = this.getGroupCols();
		if (groupCols == null) {
			throw new RuntimeException("groupCols must be not empty.");
		}

		final Params params = getParams();

		String[] colNames = in.getColNames();
		TypeInformation <?>[] colTypes = in.getColTypes();

		final HandleInvalidMethod handleInvalid = getHandleInvalidMethod();
		final TypeInformation <?>[] groupColTypes = TableUtil.findColTypesWithAssertAndHint(in.getSchema(), groupCols);
		final int[] groupColIndices = TableUtil.findColIndicesWithAssertAndHint(in.getColNames(), groupCols);

		String[] zippedColsOpt = getSelectedCols();

		if (zippedColsOpt == null) {
			List <String> zippedColsList = new ArrayList <>();

			for (String col : colNames) {
				if (TableUtil.findColIndex(groupCols, col) == -1) {
					zippedColsList.add(col);
				}
			}

			zippedColsOpt = zippedColsList.toArray(new String[0]);
		}

		final String[] zippedCols = zippedColsOpt;
		final TypeInformation <?>[] zipColTypes = TableUtil.findColTypesWithAssertAndHint(in.getSchema(), zippedCols);
		final int[] zipeedColIndices = TableUtil.findColIndicesWithAssertAndHint(colNames, zippedCols);

		final PandasUdfMapper pandasUdfMapper = new PandasUdfMapper(
			new TableSchema(
				ArrayUtils.add(groupCols, MTABLE_COL_NAME),
				ArrayUtils.add(groupColTypes, AlinkTypes.M_TABLE)
			),
			params.clone().set(PandasUdfParams.SELECTED_COLS, new String[] {MTABLE_COL_NAME})
		);

		final int numThreads = getNumThreads();

		this.setOutput(
			in.getDataSet()
				.groupBy(groupColIndices)
				.reduceGroup(new RichGroupReduceFunction <Row, Row>() {
					MapperAdapterMT mapperAdapterMT;

					@Override
					public void open(Configuration parameters) throws Exception {
						LOG.info("Entering open");
						super.open(parameters);
						mapperAdapterMT = new MapperAdapterMT(pandasUdfMapper, numThreads);
						mapperAdapterMT.open(parameters);
						LOG.info("Leaving open");
					}

					@Override
					public void close() throws Exception {
						LOG.info("Entering close");
						mapperAdapterMT.close();
						super.close();
						LOG.info("Leaving close");
					}

					@Override
					public void reduce(Iterable <Row> values, Collector <Row> out) throws Exception {
						LOG.info("Entering reduce");
						List <Row> rows = new ArrayList <>(1000);
						for (Row value : values) {
							rows.add(value);
						}
						if (rows.isEmpty()) {
							return;
						}
						LOG.info("rows.size() = {}", rows.size());
						Row first = rows.get(0);
						Row buffer = new Row(groupColIndices.length + 1);
						for (int i = 0; i < groupColIndices.length; ++i) {
							buffer.setField(i, first.getField(groupColIndices[i]));
						}

						buffer.setField(
							groupColIndices.length,
							new MTable(
								rows.stream()
									.map(row -> Row.project(row, zipeedColIndices))
									.collect(Collectors.toList()),
								zippedCols,
								zipColTypes
							)
						);

						try {
							mapperAdapterMT.flatMap(buffer, out);
						} catch (Exception e) {
							switch (handleInvalid) {
								case SKIP:
									break;
								case ERROR:
								default:
									throw new RuntimeException(e);
							}
						}
						LOG.info("Leaving mapPartition");
					}
				}),
			pandasUdfMapper.getOutputSchema()
		);

		return (T) this;
	}
}
