package com.alibaba.alink.params.sql;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface SqlCmdParams<T> extends WithParams <T> {

	/**
	 * Param "command"
	 */
	ParamInfo <String> COMMAND = ParamInfoFactory
		.createParamInfo("command", String.class)
		.setDescription("command")
		.setRequired()
		.build();
	/**
	 * Param "alias"
	 */
	ParamInfo <String[]> ALIAS = ParamInfoFactory
		.createParamInfo("alias", String[].class)
		.setDescription("alias")
		.setRequired()
		.build();

	default String getCommand() {return get(COMMAND);}

	default T setCommand(String value) {return set(COMMAND, value);}

	default String[] getAlias() {return get(ALIAS);}

	default T setAlias(String[] value) {return set(ALIAS, value);}
}
