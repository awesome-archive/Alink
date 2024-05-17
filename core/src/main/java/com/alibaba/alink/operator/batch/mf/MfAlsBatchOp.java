package com.alibaba.alink.operator.batch.mf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.AlsModelInfoBatchOp;
import com.alibaba.alink.operator.common.recommendation.HugeMfAlsImpl;
import com.alibaba.alink.params.recommendation.MfAlsParams;

import static com.alibaba.alink.operator.batch.recommendation.AlsModelInfoBatchOp.ITEM_NAME;
import static com.alibaba.alink.operator.batch.recommendation.AlsModelInfoBatchOp.USER_NAME;

/**
 * Matrix factorization using ALS algorithrim. Support both implicit and explicit model.
 */

@Internal
@InputPorts(values = {
	@PortSpec(PortType.DATA),
	@PortSpec(value = PortType.MODEL, isOptional = true)
})
@OutputPorts(values = {
	@PortSpec(value = PortType.DATA, desc = PortDesc.USER_FACTOR),
	@PortSpec(value = PortType.DATA, desc = PortDesc.ITEM_FACTOR),
	@PortSpec(value = PortType.DATA, desc = PortDesc.APPEND_USER_FACTOR, isOptional = true),
	@PortSpec(value = PortType.DATA, desc = PortDesc.APPEND_ITEM_FACTOR, isOptional = true)
})
@ParamSelectColumnSpec(name = "userCol")
@ParamSelectColumnSpec(name = "itemCol")
@ParamSelectColumnSpec(name = "rateCol",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("矩阵分解")
public final class 	MfAlsBatchOp extends BatchOperator <MfAlsBatchOp>
	implements MfAlsParams <MfAlsBatchOp> {

	private static final long serialVersionUID = 3116407570842047714L;

	public MfAlsBatchOp() {
		this(new Params());
	}

	public MfAlsBatchOp(Params params) {
		super(params);
	}

	@Override
	public MfAlsBatchOp linkFrom(BatchOperator <?>... inputs) {
		final String userColName = getUserCol();
		final String itemColName = getItemCol();
		boolean implicit = getParams().get(IMPLICIT_PREFS);
		BatchOperator <?> in;

		if (inputs.length == 1) {
			in = inputs[0];
			Tuple2 <BatchOperator <?>, BatchOperator <?>> factors = HugeMfAlsImpl.factorize(in, getParams(), implicit);
			this.setOutputTable(factors.f0.getOutputTable());
			this.setSideOutputTables(new Table[] {factors.f1.getOutputTable()});
		} else if (inputs.length == 2) {
			in = inputs[1];
			AlsModelInfoBatchOp modelInfo = new AlsModelInfoBatchOp(getParams()).linkFrom(inputs[0]);
			BatchOperator <?> initUserEmbedding
				= modelInfo.getUserEmbedding().select(USER_NAME + " as " + userColName + ", factors");
			BatchOperator <?> initItemEmbedding
				= modelInfo.getItemEmbedding().select(ITEM_NAME + " as " + itemColName + ", factors");
			Tuple4 <BatchOperator <?>, BatchOperator <?>, BatchOperator <?>, BatchOperator <?>>
				factors = HugeMfAlsImpl.factorize(initUserEmbedding, initItemEmbedding, in, getParams(), implicit);
			this.setOutputTable(factors.f0.getOutputTable());
			this.setSideOutputTables(new Table[] {factors.f1.getOutputTable(),
				factors.f2.getOutputTable(), factors.f3.getOutputTable()});
		} else {
			throw new AkIllegalArgumentException("als input op count err, need 1 or 2 input op.");
		}
		return this;
	}
}
