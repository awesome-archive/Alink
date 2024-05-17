package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.AlinkGlobalConfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author chengjie
 */
public class GraphUtils {

	// tuple3: numUsers, numItems, numEdges
	public static DataSet <Tuple3 <Integer, Integer, Long>> getGraphCounts(
		DataSet <Tuple3 <Integer, Integer, Float>> edges) {
		return edges
			. <Tuple3 <Integer, Integer, Long>>mapPartition(
				new MapPartitionFunction <Tuple3 <Integer, Integer, Float>, Tuple3 <Integer, Integer, Long>>() {
					private static final long serialVersionUID = -8232022897844991783L;

					@Override
					public void mapPartition(Iterable <Tuple3 <Integer, Integer, Float>> edges,
											 Collector <Tuple3 <Integer, Integer, Long>> out) throws Exception {
						int maxUserId = 0;
						int maxItemId = 0;
						long cnt = 0L;
						for (Tuple3 <Integer, Integer, Float> edge : edges) {
							maxUserId = Math.max(edge.f0, maxUserId);
							maxItemId = Math.max(edge.f1, maxItemId);
							cnt++;
						}
						out.collect(new Tuple3 <>(maxUserId, maxItemId, cnt));
					}
				})
			.reduce(new ReduceFunction <Tuple3 <Integer, Integer, Long>>() {
				private static final long serialVersionUID = 6751361566666304252L;

				@Override
				public Tuple3 <Integer, Integer, Long> reduce(Tuple3 <Integer, Integer, Long> value1,
															  Tuple3 <Integer, Integer, Long> value2) throws
					Exception {
					return new Tuple3 <>(Math.max(value1.f0, value2.f0), Math.max(value1.f1, value2.f1),
						value1.f2 + value2.f2);
				}
			})
			.map(new MapFunction <Tuple3 <Integer, Integer, Long>, Tuple3 <Integer, Integer, Long>>() {
				private static final long serialVersionUID = -4530052181307187847L;

				@Override
				public Tuple3 <Integer, Integer, Long> map(Tuple3 <Integer, Integer, Long> value) throws Exception {
					if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
						System.out.println("num user = " + (value.f0 + 1));
						System.out.println("num item = " + (value.f1 + 1));
						System.out.println("num edge = " + (value.f2));
					}
					return new Tuple3 <>(value.f0 + 1, value.f1 + 1, value.f2);
				}
			});
	}

	/**
	 * @param edges
	 * @param graphCounts
	 * @param numBlocks
	 * @param who:        0->userGraph, 1->itemGraph
	 * @return tuple4: blockId, nodeId, nodeNeighbors, nodeNeighborWeights
	 */
	public static DataSet <Tuple4 <Integer, Integer, int[], float[]>>
	constructGraph(DataSet <Tuple3 <Integer, Integer, Float>> edges,
				   DataSet <Tuple3 <Integer, Integer, Long>> graphCounts,
				   int numBlocks,
				   int who) {
		final int src = (who == 0 ? 0 : 1);
		final int dst = (who == 0 ? 1 : 0);

		return edges
			.groupBy(src)
			.sortGroup(dst, Order.ASCENDING)
			.reduceGroup(new ConstructGraphRowOp(src, dst, numBlocks))
			.withBroadcastSet(graphCounts, "graphCounts")
			.partitionCustom(new CustomBlockPartitioner(), 0);
	}

	public static class BlockPartitioner {
		public static int[] getBlockStarts(int numBlocks, int numElements) {
			int blockSize = numElements / numBlocks;
			int remain = numElements % numBlocks;
			int[] starts = new int[numBlocks];
			for (int i = 0; i < numBlocks; i++) {
				starts[i] = i * blockSize + Math.min(i, remain);
			}
			return starts;
		}

		public static int[] getBlockCounts(int numBlocks, int numElements) {
			int blockSize = numElements / numBlocks;
			int remain = numElements % numBlocks;
			int[] count = new int[numBlocks];
			for (int i = 0; i < numBlocks; i++) {
				count[i] = blockSize + (i < remain ? 1 : 0);
			}
			return count;
		}

		public static int getBlockId(int[] blockStarts, int elementId) {
			int pos = Arrays.binarySearch(blockStarts, elementId + 1);
			if (pos < 0) {
				pos = -(pos + 1);
			} else if (pos > 0) {
				// to handle a special case that numElements > numBlocks
				while (pos > 0 && blockStarts[pos] == blockStarts[pos - 1]) { pos--; }
			} else {
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println(new Tuple1 <int[]>(blockStarts));
					System.out.println("elementId = " + elementId);
				}
				throw new RuntimeException("unexpected");
			}
			return pos - 1;
		}
	}

	public static class CustomBlockPartitioner implements Partitioner <Integer> {
		private static final long serialVersionUID = -6562122859494588568L;

		@Override
		public int partition(Integer key, int numPartitions) {
			return key % numPartitions;
		}
	}

	private static class ConstructGraphRowOp extends RichGroupReduceFunction <
		Tuple3 <Integer, Integer, Float>, Tuple4 <Integer, Integer, int[], float[]>> {
		private static final long serialVersionUID = -1009468987728999536L;
		private int src;
		private int dst;
		private List <Tuple3 <Integer, Integer, Long>> graphCounts = null;
		private int numBlocks;
		private int[] srcStarts = null;
		private int[] srcCounts = null;

		public ConstructGraphRowOp(int src, int dst, int numBlocks) {
			this.src = src;
			this.dst = dst;
			this.numBlocks = numBlocks;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			this.graphCounts = getRuntimeContext().getBroadcastVariable("graphCounts");
			int numSrcs = graphCounts.get(0).getField(src);
			this.srcStarts = BlockPartitioner.getBlockStarts(numBlocks, numSrcs);
			this.srcCounts = BlockPartitioner.getBlockCounts(numBlocks, numSrcs);
		}

		@Override
		public void reduce(Iterable <Tuple3 <Integer, Integer, Float>> values,
						   Collector <Tuple4 <Integer, Integer, int[], float[]>> out) throws Exception {
			List <Tuple3 <Integer, Integer, Float>> buffer = new ArrayList <>();
			for (Tuple3 <Integer, Integer, Float> v : values) { buffer.add(v); }

			int[] neighbors = new int[buffer.size()];
			float[] ratings = new float[buffer.size()];

			int pos = 0;
			int srcNodeId = -1;
			for (Tuple3 <Integer, Integer, Float> v : buffer) {
				srcNodeId = v.getField(src);
				neighbors[pos] = v.getField(dst);
				ratings[pos] = v.getField(2);
				pos++;
			}

			int srcBlockId = BlockPartitioner.getBlockId(srcStarts, srcNodeId);
			out.collect(new Tuple4 <>(srcBlockId, srcNodeId, neighbors, ratings));
		}
	}

}
