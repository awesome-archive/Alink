package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.batch.utils.WithModelInfoBatchOp;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.AlsModelInfo;
import com.alibaba.alink.operator.common.utils.PackBatchOperatorUtil;
import com.alibaba.alink.params.recommendation.VecDotModelGeneratorParams;

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
@NameCn("VecDot 推荐模型训练")
@NameEn("VecDot Recommendation Train")
public final class VecDotModelGeneratorBatchOp
	extends BatchOperator <VecDotModelGeneratorBatchOp>
	implements VecDotModelGeneratorParams <VecDotModelGeneratorBatchOp>,
	WithModelInfoBatchOp <AlsModelInfo, VecDotModelGeneratorBatchOp, VecDotModelGeneratorBatchOp.AlsModelInfoBatchOp> {

	public VecDotModelGeneratorBatchOp() {
		this(new Params());
	}

	public VecDotModelGeneratorBatchOp(Params params) {
		super(params);
	}

	@Override
	public VecDotModelGeneratorBatchOp linkFrom(List <BatchOperator <?>> ins) {
		return linkFrom(ins.get(0));
	}

	public static class AlsModelInfoBatchOp extends ExtractModelInfoBatchOp <AlsModelInfo, AlsModelInfoBatchOp> {
		public AlsModelInfoBatchOp(Params params) {
			super(params);
		}

		@Override
		public AlsModelInfo createModelInfo(List <Row> rows) {
			return new AlsModelInfo(0, 0, 0, getParams());
		}
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
	public VecDotModelGeneratorBatchOp linkFrom(BatchOperator <?>... inputs) {
		checkMinOpSize(2, inputs);

		final String userIdColName = getUserIdCol();
		final String userVecColName = getUserVecCol();
		final String itemIdColName = getItemIdCol();
		final String itemVecColName = getItemVecCol();

		BatchOperator <?> pairs = null;
		if (inputs.length > 2) {
			pairs = inputs[2].select(new String[] {userIdColName, itemIdColName});
		}

		BatchOperator model = PackBatchOperatorUtil.packBatchOps(
			new BatchOperator <?>[] {
				inputs[0].select(new String[] {userIdColName, userVecColName}),
				inputs[1].select(new String[] {itemIdColName, itemVecColName}),
				pairs
			}
		);
		this.setOutputTable(model.getOutputTable());
		return this;
	}

}
