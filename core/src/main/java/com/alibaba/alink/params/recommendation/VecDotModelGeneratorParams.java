package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface VecDotModelGeneratorParams<T> extends WithParams <T> {

	@NameCn("UserId列列名")
	@DescCn("UserId列列名")
	ParamInfo <String> USER_ID_COL = ParamInfoFactory
		.createParamInfo("userIdCol", String.class)
		.setAlias(new String[] {"userIdColName"})
		.setDescription("User id column name")
		.setRequired()
		.build();

	default String getUserIdCol() {
		return get(USER_ID_COL);
	}

	default T setUserIdCol(String value) {
		return set(USER_ID_COL, value);
	}

	@NameCn("UserVec列列名")
	@DescCn("UserVec列列名")
	ParamInfo <String> USER_VEC_COL = ParamInfoFactory
		.createParamInfo("userVecCol", String.class)
		.setAlias(new String[] {"userVecColName"})
		.setDescription("User vector column name")
		.setRequired()
		.build();

	default String getUserVecCol() {
		return get(USER_VEC_COL);
	}

	default T setUserVecCol(String value) {
		return set(USER_VEC_COL, value);
	}

	@NameCn("ItemId列列名")
	@DescCn("ItemId列列名")
	ParamInfo <String> ITEM_ID_COL = ParamInfoFactory
		.createParamInfo("itemIdCol", String.class)
		.setAlias(new String[] {"itemIdColName"})
		.setDescription("Item id column name")
		.setRequired()
		.build();

	default String getItemIdCol() {
		return get(ITEM_ID_COL);
	}

	default T setItemIdCol(String value) {
		return set(ITEM_ID_COL, value);
	}

	@NameCn("ItemVec列列名")
	@DescCn("ItemVec列列名")
	ParamInfo <String> ITEM_VEC_COL = ParamInfoFactory
		.createParamInfo("itemVecCol", String.class)
		.setAlias(new String[] {"itemVecColName"})
		.setDescription("Item vector column name")
		.setRequired()
		.build();

	default String getItemVecCol() {
		return get(ITEM_VEC_COL);
	}

	default T setItemVecCol(String value) {
		return set(ITEM_VEC_COL, value);
	}

}
