package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.RMapper;
import com.alibaba.alink.params.dataproc.GroupRParams;
import com.alibaba.alink.params.dataproc.RUdfParams;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@NameCn("GroupRBatchOp")
@NameEn("GroupRBatchOp")
public class GroupRBatchOp extends BatchOperator <GroupRBatchOp>
	implements GroupRParams <GroupRBatchOp> {

	private final static String MTABLE_COL_NAME = "grouped_data_1_col_mtable";

	public GroupRBatchOp() {
		this(null);
	}

	public GroupRBatchOp(Params params) {
		super(params);
	}

	@Override
	public GroupRBatchOp linkFrom(BatchOperator <?>... inputs) {
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

		final RMapper rMapper = new RMapper(
			new TableSchema(
				ArrayUtils.add(groupCols, MTABLE_COL_NAME),
				ArrayUtils.add(groupColTypes, AlinkTypes.M_TABLE)
			),
			params.clone()
				.set(RUdfParams.SELECTED_COL, MTABLE_COL_NAME)
				.set(HasReservedColsDefaultAsNull.RESERVED_COLS, groupCols)
		);

		this.setOutput(
			in.getDataSet()
				.partitionByHash(groupColIndices)
				.mapPartition(
					new RichMapPartitionFunction <Row, Row>() {

						@Override
						public void open(Configuration parameters) throws Exception {
							super.open(parameters);

							rMapper.open();
						}

						@Override
						public void close() throws Exception {
							rMapper.close();

							super.close();
						}

						@Override
						public void mapPartition(Iterable <Row> values, Collector <Row> out) throws Exception {
							List <Row> rows = new ArrayList <>(1000);

							for (Row row : values) {
								rows.add(row);
							}

							new MTable(rows, colNames, colTypes)
								.reduceGroup(groupColIndices, localRows -> {
									if (localRows.isEmpty()) {
										return;
									}

									Row first = localRows.get(0);

									Row buffer = new Row(groupColIndices.length + 1);

									for (int i = 0; i < groupColIndices.length; ++i) {
										buffer.setField(i, first.getField(groupColIndices[i]));
									}

									buffer.setField(
										groupColIndices.length,
										new MTable(
											localRows
												.stream()
												.map(row -> Row.project(row, zipeedColIndices))
												.collect(Collectors.toList()),
											zippedCols,
											zipColTypes
										)
									);

									try {
										out.collect(rMapper.map(buffer));
									} catch (Exception e) {
										switch (handleInvalid) {
											case SKIP:
												break;
											case ERROR:
											default:
												throw new RuntimeException(e);
										}
									}
								});
						}
					}
				),
			rMapper.getOutputSchema()
		);
		return this;
	}
}
