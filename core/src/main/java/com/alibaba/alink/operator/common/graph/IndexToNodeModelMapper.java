package com.alibaba.alink.operator.common.graph;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.dataproc.LookupParams;
import com.alibaba.alink.params.graph.NodeToIndexParams;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Model Mapper for Node to Index operation.
 */
public class IndexToNodeModelMapper extends ModelMapper {

	private ConcurrentHashMap <Integer, Object> mapModel;

	public IndexToNodeModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);

		String[] selectedColNames = params.get(LookupParams.SELECTED_COLS);
		TableUtil.assertNumericalCols(dataSchema, selectedColNames);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		this.mapModel = new ConcurrentHashMap <>(modelRows.size());
		for (Row row : modelRows) {
			this.mapModel.put((Integer) row.getField(1), row.getField(0));
		}
	}

	@Override
	public ModelMapper createNew(List <Row> newModelRows) {
		ConcurrentHashMap <Integer, Object> newMapModel = new ConcurrentHashMap <>(mapModel.size());

		for (Row row : newModelRows) {
			newMapModel.put((Integer) row.getField(1), row.getField(0));
		}
		this.mapModel = newMapModel;
		return this;
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema modelSchema, TableSchema dataSchema, Params params) {

		String[] selectedColNames = params.get(NodeToIndexParams.SELECTED_COLS);

		String[] outputColNames = params.get(NodeToIndexParams.OUTPUT_COLS);
		if (null == outputColNames) {
			outputColNames = selectedColNames;
		}

		TypeInformation <?>[] outputColTypes = new TypeInformation[outputColNames.length];
		for (int i = 0; i < outputColTypes.length; i++) {
			outputColTypes[i] = modelSchema.getFieldTypes()[0];
		}

		return Tuple4.of(selectedColNames, outputColNames, outputColTypes,
			params.get(NodeToIndexParams.RESERVED_COLS));
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		for (int i = 0; i < selection.length(); i++) {
			result.set(i, this.mapModel.get(((Number) selection.get(i)).intValue()));
		}
	}
}
