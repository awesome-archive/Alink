package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.api.java.tuple.Tuple2;
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
import com.alibaba.alink.operator.batch.utils.WithModelInfoBatchOp;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.AlsModelInfo;
import com.alibaba.alink.operator.common.recommendation.HugeMfAlsImplForHotPoint;
import com.alibaba.alink.operator.common.utils.PackBatchOperatorUtil;
import com.alibaba.alink.params.recommendation.AlsTrainParams;

import java.util.List;

/**
 * Matrix factorization using Alternating Least Square method.
 * <p>
 * ALS tries to decompose a matrix R as R = X * Yt. Here X and Y are called factor matrices.
 * Matrix R is usually a sparse matrix representing ratings given from users to items.
 * ALS tries to find X and Y that minimize || R - X * Yt ||^2. This is done by iterations.
 * At each step, X is fixed and Y is solved, then Y is fixed and X is solved.
 * <p>
 * The algorithm is described in "Large-scale Parallel Collaborative Filtering for the Netflix Prize, 2007"
 * <p>
 * We also support implicit preference model described in
 * "Collaborative Filtering for Implicit Feedback Datasets, 2008"
 */

@Internal
@InputPorts(values = {
    @PortSpec(PortType.DATA)
})
@OutputPorts(values = {
    @PortSpec(PortType.MODEL),
    @PortSpec(value = PortType.DATA, desc = PortDesc.USER_FACTOR),
    @PortSpec(value = PortType.DATA, desc = PortDesc.ITEM_FACTOR)
})
@ParamSelectColumnSpec(name = "userCol")
@ParamSelectColumnSpec(name = "itemCol")
@ParamSelectColumnSpec(name = "rateCol",
    allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("热点 ALS 矩阵分解")
public final class AlsForHotPointTrainBatchOp
        extends BatchOperator<AlsForHotPointTrainBatchOp>
        implements AlsTrainParams<AlsForHotPointTrainBatchOp>,
        WithModelInfoBatchOp<AlsModelInfo, AlsForHotPointTrainBatchOp, AlsModelInfoBatchOp> {

    private static final long serialVersionUID = 135071766504939341L;

    public AlsForHotPointTrainBatchOp() {
        this(new Params());
    }

    public AlsForHotPointTrainBatchOp(Params params) {
        super(params);
    }

    @Override
    public AlsForHotPointTrainBatchOp linkFrom(List<BatchOperator<?>> ins) {
        return linkFrom(ins.get(0));
    }

    @Override
    public AlsModelInfoBatchOp getModelInfoBatchOp() {
        return new AlsModelInfoBatchOp(this.getParams()).linkFrom(this);
    }

    /**
     * Matrix decomposition using ALS algorithm.
     *
     * @param inputs a dataset of user-item-rating tuples
     * @return user factors and item factors.
     */
    @Override
    public AlsForHotPointTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        Tuple2<BatchOperator, BatchOperator> factors;
        final String userColName = getUserCol();
        final String itemColName = getItemCol();
        BatchOperator<?> in;
        if (inputs.length == 1) {
            in = inputs[0];
            factors = HugeMfAlsImplForHotPoint.factorize(in, getParams(), false);
        } else if (inputs.length == 2) {
            in = inputs[1];
            AlsModelInfoBatchOp modelInfo = new AlsModelInfoBatchOp(getParams()).linkFrom(inputs[0]);
            factors = HugeMfAlsImplForHotPoint.factorize(modelInfo.getUserEmbedding(),
                    modelInfo.getItemEmbedding(), in, getParams(), false);
        } else {
            throw new AkIllegalArgumentException("als input op count err, need 1 or 2 input op.");
        }

        BatchOperator userFactors = factors.f0;
        BatchOperator itemFactors = factors.f1;

        BatchOperator[] outputs = new BatchOperator[]{factors.f0, factors.f1,
                in.select(new String[]{userColName, itemColName})};
        BatchOperator model = PackBatchOperatorUtil.packBatchOps(outputs);
        this.setOutputTable(model.getOutputTable());
        this.setSideOutputTables(new Table[]{userFactors.getOutputTable(), itemFactors.getOutputTable()});
        return this;
    }
}
