package com.alibaba.alink.operator.local.sql;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import com.alibaba.alink.operator.local.source.TableSourceLocalOp;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

public class FilterLocalOpTest {
	@Test
	public void testFilterLocalOp() {
		List <Row> df = Arrays.asList(
			Row.of("Ohio", 2000, 1.5),
			Row.of("Ohio", 2001, 1.7),
			Row.of("Ohio", 2002, 3.6),
			Row.of("Nevada", 2001, 2.4),
			Row.of("Nevada", 2002, 2.9),
			Row.of("Nevada", 2003, 3.2)
		);
		LocalOperator <?> source = new TableSourceLocalOp(new MTable(df, "f1 string, f2 int, f3 double"));

		source.link(new FilterLocalOp().setClause("f1='Ohio'")).print();
		source.link(new FilterLocalOp().setClause("f2<=2001")).print();
	}

	@Test
	public void testFilterLocalOp2() {
		List <Row> df = Arrays.asList(
			Row.of("Ohio", 2000, 1.5),
			Row.of("Ohio", 2001, 1.7),
			Row.of("Ohio", 2002, 3.6),
			Row.of("Nevada", 2001, 2.4),
			Row.of("Nevada", 2002, 2.9),
			Row.of("Nevada", 2003, 3.2)
		);
		LocalOperator <?> source = new TableSourceLocalOp(new MTable(df, "f1 string, f2 int, f3 double"));

		source.link(new FilterLocalOp().setClause("abs(f3)<2.5")).print();
	}

	@Test
	public void testFilterLocalOpWithTs() {
		// calcite zone is UTC.
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		String[] colNames = new String[] {"id", "user", "sell_time", "price"};

		LocalOperator <?> source = new MemSourceLocalOp(
			new Row[] {
				Row.of(1, "user2", Timestamp.valueOf("2021-01-01 00:01:00"), 20),
				Row.of(2, "user1", Timestamp.valueOf("2021-01-01 00:02:00"), 50),
				Row.of(3, "user2", Timestamp.valueOf("2021-01-01 00:03:00"), 30),
				Row.of(4, "user1", Timestamp.valueOf("2021-01-01 00:06:00"), 60),
				Row.of(5, "user2", Timestamp.valueOf("2021-01-01 00:06:00"), 40),
				Row.of(6, "user2", Timestamp.valueOf("2021-01-01 00:06:00"), 20),
				Row.of(7, "user2", Timestamp.valueOf("2021-01-01 00:07:00"), 70),
				Row.of(8, "user1", Timestamp.valueOf("2021-01-01 00:08:00"), 80),
				Row.of(9, "user1", Timestamp.valueOf("2021-01-01 00:09:00"), 40),
				Row.of(10, "user1", Timestamp.valueOf("2021-01-01 00:10:00"), 20),
				Row.of(11, "user1", Timestamp.valueOf("2021-01-01 00:11:00"), 30),
				Row.of(12, "user1", Timestamp.valueOf("2021-01-01 00:11:00"), 50)
			},
			colNames
		);

		source.filter("unix_timestamp(sell_time)=1609430760")
			.print("---unix_timestamp---");
		source.filter("sell_time=CAST('2021-01-01 00:06:00' AS TIMESTAMP)")
			.print("---CAST---");
		source.filter("sell_time=TIMESTAMP'2021-01-01 00:06:00'")
			.print("---CAST 2---");
		source.filter("sell_time=to_timestamp('2021-01-01 00:06:00', 'yyyy-MM-dd hh:mm:ss')")
			.print("---to_timestamp---");

	}

}