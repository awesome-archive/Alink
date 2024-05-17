package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class NodeIndexerTrainBatchOpTest extends AlinkTestBase {

	List <Row> originalData;
	List <Tuple2 <String, Long>> expectedIdMapping;
	List <Row> expectedMappedDataSet;
	List <long[]> randomWalks;
	List <String> expectedRemappedString;

	@Before
	public void before() {
		originalData = new ArrayList <>();
		originalData.add(Row.of("Alice", "Lisa", 1.));
		originalData.add(Row.of("Lisa", "Karry", 2.));
		originalData.add(Row.of("Karry", "Bella", 3.));
		originalData.add(Row.of("Bella", "Lucy", 4.));
		originalData.add(Row.of("Lucy", "Bob", 5.));
		originalData.add(Row.of("John", "Bob", 6.));
		originalData.add(Row.of("John", "Stella", 7.));
		originalData.add(Row.of("John", "Kate", 8.));
		originalData.add(Row.of("Kate", "Stella", 9.));
		originalData.add(Row.of("Kate", "Jack", 10.));
		originalData.add(Row.of("Jess", "Jack", 11.));

		expectedIdMapping = new ArrayList <>();
		expectedIdMapping.add(Tuple2.of("John", 7L));
		expectedIdMapping.add(Tuple2.of("Karry", 9L));
		expectedIdMapping.add(Tuple2.of("Lisa", 13L));
		expectedIdMapping.add(Tuple2.of("Bella", 0L));
		expectedIdMapping.add(Tuple2.of("Bob", 2L));
		expectedIdMapping.add(Tuple2.of("Stella", 4L));
		expectedIdMapping.add(Tuple2.of("Alice", 1L));
		expectedIdMapping.add(Tuple2.of("Jack", 3L));
		expectedIdMapping.add(Tuple2.of("Jess", 5L));
		expectedIdMapping.add(Tuple2.of("Kate", 11L));
		expectedIdMapping.add(Tuple2.of("Lucy", 15L));

		expectedMappedDataSet = new ArrayList <>();
		expectedMappedDataSet.add(Row.of(13L, 9L, 2.0));
		expectedMappedDataSet.add(Row.of(1L, 13L, 1.0));
		expectedMappedDataSet.add(Row.of(9L, 0L, 3.0));
		expectedMappedDataSet.add(Row.of(7L, 2L, 6.0));
		expectedMappedDataSet.add(Row.of(15L, 2L, 5.0));
		expectedMappedDataSet.add(Row.of(7L, 4L, 7.0));
		expectedMappedDataSet.add(Row.of(11L, 4L, 9.0));
		expectedMappedDataSet.add(Row.of(5L, 3L, 11.0));
		expectedMappedDataSet.add(Row.of(11L, 3L, 10.0));
		expectedMappedDataSet.add(Row.of(7L, 11L, 8.0));
		expectedMappedDataSet.add(Row.of(0L, 15L, 4.0));

		randomWalks = new ArrayList <>();
		randomWalks.add(new long[] {1, 13, 9, 0, 15});
		randomWalks.add(new long[] {15, 2, 7, 4});
		randomWalks.add(new long[] {7, 11, 3, 5});

		expectedRemappedString = new ArrayList <>();
		expectedRemappedString.add("Alice Lisa Karry Bella Lucy");
		expectedRemappedString.add("Lucy Bob John Stella");
		expectedRemappedString.add("John Kate Jack Jess");
	}

	@Test
	public void test1() throws Exception {
		func(
			new MemSourceBatchOp(originalData, "source string, target string, value double")
		);
	}

	@Test
	public void test2() throws Exception {
		func(
			new MemSourceBatchOp(expectedMappedDataSet, "source long, target long, value double")
		);
	}

	private void func(BatchOperator <?> input) throws Exception {
		input.lazyPrint("\n< input >");

		BatchOperator <?> nodeIndexer = new NodeIndexerTrainBatchOp()
			.setSelectedCols("source", "target")
			.linkFrom(input);

		nodeIndexer.lazyPrint("\n< nodeIndexer >");

		BatchOperator <?> nodeToIndex = new NodeToIndexBatchOp()
			.setSelectedCols("source", "target")
			.linkFrom(nodeIndexer, input);

		nodeToIndex.lazyPrint("\n< nodeToIndex >");

		new IndexToNodeBatchOp()
			.setSelectedCols("source", "target")
			.linkFrom(nodeIndexer, nodeToIndex)
			.print("\n< indexToNode >");
	}

}