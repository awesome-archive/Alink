package com.alibaba.alink.pipeline.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.operator.common.recommendation.VecDotRecommKernel;
import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;

@NameCn("VecDot: ItemsPerUser推荐")
public class VecDotItemsPerUserRecommender
	extends BaseRecommender <VecDotItemsPerUserRecommender>
	implements BaseItemsPerUserRecommParams <VecDotItemsPerUserRecommender> {

	public VecDotItemsPerUserRecommender() {
		this(null);
	}

	public VecDotItemsPerUserRecommender(Params params) {
		super(VecDotRecommKernel::new, RecommType.ITEMS_PER_USER, params);
	}
}
