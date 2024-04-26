package com.alibaba.alink.operator.local.sql;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class TimestampLocalSqlTest {
	// for 1.9, local sql for timestamp has something wrong, only support in 1.13.
	@Ignore
	@Test
	public void testOrderByTs() {
		LocalOperator <?> source = getSourceWithTs();

		source
			.orderBy("sell_time", 3)
			.print();
	}

	@Test
	public void testGroupByTs() {
		LocalOperator<?> source = getSourceWithTs();
		source
			.groupBy("sell_time", "sell_time, "
				+ "sum(price) as sum_price")
			.print();
	}



	@Test
	public void testSelectTs() {
		LocalOperator<?> source = getSourceWithTs();
		source
			.select("sell_time, price as sum_price")
			.print();
	}

	@Ignore
	@Test
	public void test1() {
		LocalOperator<?> source = getSourceWithTs();
		source.print();

		source.select("*, price + 1 as f0").print();
		source.select("date_format_ltz(sell_time) as c1").print();
		source
			.select("id,TIMESTAMP'1970-01-01 00:00:00.012' as col2, CAST('1970-01-01 00:00:00.012' AS TIMESTAMP) as col3,price")
			.print();
	}

	@Ignore
	@Test
	public void testDistinct() {
		LocalOperator<?> source = getSourceWithTs();
		source
			.distinct()
			.print();
	}

	@Ignore
	@Test
	public void testFilter() {
		LocalOperator<?> source = getSourceWithTs();

		source
			.filter("unix_timestamp_macro(sell_time)=1000")
			.filter("sell_time=CAST('1970-01-01 00:00:01' AS TIMESTAMP)")
			.filter("sell_time=TIMESTAMP'1970-01-01 00:00:01'")
			.filter("sell_time=to_timestamp_from_format('1970-01-01 00:00:01', 'yyyy-MM-dd hh:mm:ss')")
			.print();
	}

	//@Test
	//public void testIntersect() {
	//	TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
	//	LocalOperator<?> data1 = getSourceWithTs();
	//	LocalOperator<?> data2 = new UnionAllLocalOp().linkFrom(data1, data1);
	//	System.out.println(new IntersectAllLocalOp().linkFrom(data1, data2).getOutputTable().getNumRow());
	//}



	private static LocalOperator <?> getSourceWithTs() {
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		String[] colNames = new String[] {"id", "user", "sell_time", "price"};

		return new MemSourceLocalOp(
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
				Row.of(12, "user1", Timestamp.valueOf("2021-01-01 00:11:00"), 50),
				Row.of(12, "user1", new Timestamp(1000), 50),
			},
			colNames
		);
	}


}
