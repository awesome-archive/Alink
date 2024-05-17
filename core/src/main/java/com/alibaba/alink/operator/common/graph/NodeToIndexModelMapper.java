package com.alibaba.alink.operator.common.graph;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.dataproc.LookupParams;
import com.alibaba.alink.params.graph.NodeToIndexParams;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Model Mapper for Node to Index operation.
 */
public class NodeToIndexModelMapper extends ModelMapper {

	private ConcurrentHashMap <Object, Integer> mapModel;
	final private TypeInformation <?> nodeType;

	public NodeToIndexModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);

		this.nodeType = modelSchema.getFieldTypes()[0];

		String[] selectedColNames = params.get(LookupParams.SELECTED_COLS);
		for (int i = 0; i < selectedColNames.length; ++i) {
			if (TableUtil.findColTypeWithAssertAndHint(dataSchema, selectedColNames[i]) != this.nodeType) {
				throw new AkIllegalDataException("Data types are not match. selected column type is "
					+ TableUtil.findColTypeWithAssertAndHint(dataSchema, selectedColNames[i])
					+ " , and the node type is " + this.nodeType
				);
			}
		}
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		this.mapModel = new ConcurrentHashMap <>(modelRows.size());
		for (Row row : modelRows) {
			this.mapModel.put(row.getField(0), (Integer) row.getField(1));
		}
	}

	@Override
	public ModelMapper createNew(List <Row> newModelRows) {
		ConcurrentHashMap <Object, Integer> newMapModel = new ConcurrentHashMap <>(mapModel.size());

		for (Row row : newModelRows) {
			newMapModel.put(row.getField(0), (Integer) row.getField(1));
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
			outputColTypes[i] = AlinkTypes.INT;
		}

		return Tuple4.of(selectedColNames, outputColNames, outputColTypes,
			params.get(NodeToIndexParams.RESERVED_COLS));
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		for (int i = 0; i < selection.length(); i++) {
			result.set(i, this.mapModel.get(selection.get(i)));
		}
	}
}
