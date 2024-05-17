package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.operator.common.recommendation.VecDotRecommKernel;
import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;

@NameCn("VecDot 推荐（向user推荐items）")
@NameEn("VecDot recommendation items to user")
public class VecDotItemsPerUserRecommBatchOp
	extends BaseRecommBatchOp <VecDotItemsPerUserRecommBatchOp>
	implements BaseItemsPerUserRecommParams <VecDotItemsPerUserRecommBatchOp> {

	public VecDotItemsPerUserRecommBatchOp() {
		this(null);
	}

	public VecDotItemsPerUserRecommBatchOp(Params params) {
		super(VecDotRecommKernel::new, RecommType.ITEMS_PER_USER, params);
	}
}
