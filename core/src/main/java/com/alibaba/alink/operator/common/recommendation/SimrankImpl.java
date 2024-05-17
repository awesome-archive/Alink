package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.operator.common.dataproc.BlockwiseCross;
import com.alibaba.alink.operator.common.dataproc.FirstReducer;
import com.alibaba.alink.params.recommendation.SimrankParams;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class SimrankImpl {
	private int numWalks;
	private int walkLength;
	private int topK;
	private double decayFactor;
	private String modelType;

	public SimrankImpl(Params params) {

		this.numWalks = params.get(SimrankParams.NUM_WALKS);
		this.walkLength = params.get(SimrankParams.WALK_LENGTH);
		this.topK = params.get(SimrankParams.TOP_K);
		this.decayFactor = params.get(SimrankParams.DECAY_FACTOR);
		this.modelType = params.get(SimrankParams.MODEL_TYPE).toString();
	}

	private static int countEvidence(int[] ovlRecord, int novl, int ovlPosition) {
		int i;
		for (i = 0; i < novl; i++) {
			if (ovlPosition == ovlRecord[i]) {
				break;
			}
		}
		if (i == novl) { // not found
			ovlRecord[i] = ovlPosition;
			novl++;
		}
		return novl;
	}

	// tuple3: userId, itemId, weight
	private DataSet <Tuple3 <Integer, Integer, Float>>
	preprocess(DataSet <Tuple3 <Integer, Integer, Float>> input) {

		final boolean isSimrankPlusPlus = this.modelType.equalsIgnoreCase("simrankplusplus");

		DataSet <Tuple3 <Integer, Integer, Float>> edges = input
			.groupBy(0, 1)
			.reduce(new ReduceFunction <Tuple3 <Integer, Integer, Float>>() {
				private static final long serialVersionUID = 8265532231237152270L;

				@Override
				public Tuple3 <Integer, Integer, Float> reduce(Tuple3 <Integer, Integer, Float> value1,
															   Tuple3 <Integer, Integer, Float> value2)
					throws Exception {
					return new Tuple3 <>(value1.f0, value1.f1, isSimrankPlusPlus ? (value1.f2 + value2.f2) : 1.0F);
				}
			});

		return edges;
	}

	// tuple3: blockId, nodeId, paths
	private DataSet <Tuple3 <Integer, Integer, int[][]>>
	walk(DataSet <Tuple3 <Integer, Integer, Float>> input, int numBlocks) {
		final int N = numWalks;
		final int L = walkLength;

		DataSet <Tuple3 <Integer, Integer, Long>> graphCounts = GraphUtils.getGraphCounts(input);

		// tuple4: blockId, nodeId, neighbors, weights
		DataSet <Tuple4 <Integer, Integer, int[], float[]>> userGraph =
			GraphUtils.constructGraph(input, graphCounts, numBlocks, 0)
				.map(new NormalizeWeightsOp(numWalks, walkLength))
				.withForwardedFields("f0;f1;f2");
		DataSet <Tuple4 <Integer, Integer, int[], float[]>> itemGraph =
			GraphUtils.constructGraph(input, graphCounts, numBlocks, 1)
				.map(new NormalizeWeightsOp(numWalks, walkLength))
				.withForwardedFields("f0;f1;f2");

		// tuple3: blockId, nodeId, paths
		DataSet <Tuple3 <Integer, Integer, int[][]>> itemStat = itemGraph
			.map(new MapFunction <Tuple4 <Integer, Integer, int[], float[]>, Tuple3 <Integer, Integer, int[][]>>() {
				private static final long serialVersionUID = -3877536468429607896L;

				@Override
				public Tuple3 <Integer, Integer, int[][]> map(Tuple4 <Integer, Integer, int[], float[]> value)
					throws Exception {
					return new Tuple3 <>(value.f0, value.f1, new int[N][L]);
				}
			})
			.withForwardedFields("f0;f1");

		// Iterate
		IterativeDataSet <Tuple3 <Integer, Integer, int[][]>> loop = itemStat.iterate(walkLength / 2);
		DataSet <Tuple1 <Integer>> dummy = loop
			.reduceGroup(new FirstReducer <>(1))
			. <Tuple1 <Integer>>project(0)
			.map(new RichMapFunction <Tuple1 <Integer>, Tuple1 <Integer>>() {
				private static final long serialVersionUID = -481385552076270332L;

				@Override
				public Tuple1 <Integer> map(Tuple1 <Integer> value) throws Exception {
					if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
						System.out.println("walking at step " + getIterationRuntimeContext().getSuperstepNumber());
					}
					return value;
				}
			});

		DataSet <Tuple2 <Integer, int[]>> itemNextPos = itemGraph
			.map(new NextPosOp(modelType, numWalks, 1))
			.withBroadcastSet(dummy, "dummy");

		DataSet <Tuple3 <Integer, Integer, int[][]>> itemStep = walkOneStep(loop, itemNextPos, graphCounts, 1,
			numBlocks);

		DataSet <Tuple2 <Integer, int[]>> userNextPos = userGraph
			.map(new NextPosOp(modelType, numWalks, 0))
			.withBroadcastSet(dummy, "dummy");

		DataSet <Tuple3 <Integer, Integer, int[][]>> userStep = walkOneStep(itemStep, userNextPos, graphCounts, 0,
			numBlocks);

		DataSet <Tuple3 <Integer, Integer, int[][]>> finalPath = loop.closeWith(userStep);

		return finalPath;
	}

	private DataSet <Tuple3 <Integer, Integer, int[][]>>
	walkOneStep(DataSet <Tuple3 <Integer, Integer, int[][]>> currPos,
				DataSet <Tuple2 <Integer, int[]>> nextPos,
				DataSet <Tuple3 <Integer, Integer, Long>> graphCounts,
				int who, int numBlocks) {
		// to replace broadcast with fine grain communication
		// data are send from 'src' to 'dst'
		// step 1: generate description of what 'dst' need
		// step 2: 'src' send data to 'dst'

		int sender = who;

		// tuple: reicverBlockId, senderBlockId, N, request
		DataSet <Tuple4 <Integer, Integer, Integer, int[]>> request = currPos
			.groupBy(0)
			.withPartitioner(new GraphUtils.CustomBlockPartitioner())
			.reduceGroup(new GenerateRequestOp(sender, numBlocks, numWalks))
			.withBroadcastSet(graphCounts, "graphCounts");

		DataSet <Tuple3 <Integer, Integer, int[]>> nextPosWithBlockId = nextPos
			.map(new AppendIdToPathOp(sender, numBlocks))
			.withBroadcastSet(graphCounts, "graphCounts")
			.withForwardedFields("f0->f1;f1->f2");

		// tuple: receiverBlockId, senderBlockId, N, response
		DataSet <Tuple4 <Integer, Integer, Integer, int[]>> response = nextPosWithBlockId
			.coGroup(request).where(0).equalTo(1)
			.sortFirstGroup(1, Order.ASCENDING) // ordered by sender node id
			.withPartitioner(new GraphUtils.CustomBlockPartitioner()) // request are sent to sender
			.with(new GenerateResponseOp());

		return currPos
			.coGroup(response).where(0).equalTo(0)
			.sortSecondGroup(2, Order.ASCENDING) // ordered by N ???
			.withPartitioner(new GraphUtils.CustomBlockPartitioner()) // response are sent to receiver
			.with(new WalkOneStepOp(numWalks, who)) // receiver walk one step by querying the response
			.withForwardedFieldsFirst("f0;f1");
	}

	private static class SimilarityFunction implements BlockwiseCross.ScoreFunction <int[][], int[][]>, Serializable {
		private static final long serialVersionUID = 6368757542598205679L;
		int n;
		int l;
		double c;
		String modelType;
		transient SimrankSim simrank;

		SimilarityFunction(int n, int l, double c, String modelType) {
			this.n = n;
			this.l = l;
			this.c = c;
			this.modelType = modelType;
		}

		@Override
		public float score(Long id1, int[][] v1, Long id2, int[][] v2) {
			if (simrank == null) {
				simrank = new SimrankSim(n, l, c, modelType);
			}
			if (id1.equals(id2)) {
				return 0.F;
			}
			return (float) simrank.similarity(v1, v2);
		}
	}

	public DataSet <Tuple3 <Integer, int[], float[]>>
	batchPredict(DataSet <Tuple3 <Integer, Integer, Float>> input) {

		final String modelType = this.modelType;
		final int N = numWalks;
		final int L = walkLength;
		final double C = decayFactor;
		final int numBlocks = input.getExecutionEnvironment().getParallelism();

		DataSet <Tuple3 <Integer, Integer, Float>> processedInput = preprocess(input);

		// tuple: itemId, paths
		DataSet <Tuple2 <Long, int[][]>> paths = walk(processedInput, numBlocks)
			.map(new MapFunction <Tuple3 <Integer, Integer, int[][]>, Tuple2 <Long, int[][]>>() {
				private static final long serialVersionUID = -96658696394990068L;

				@Override
				public Tuple2 <Long, int[][]> map(Tuple3 <Integer, Integer, int[][]> value) throws Exception {
					return Tuple2.of(value.f1.longValue(), value.f2);
				}
			});

		DataSet <Tuple3 <Long, long[], float[]>> topk = BlockwiseCross.findTopK(paths, paths, topK, Order.DESCENDING,
			new SimilarityFunction(N, L, C, modelType));

		return topk
			.flatMap(new FlatMapFunction <Tuple3 <Long, long[], float[]>, Tuple3 <Integer, int[], float[]>>() {
				private static final long serialVersionUID = 717342710639052849L;

				@Override
				public void flatMap(Tuple3 <Long, long[], float[]> value,
									Collector <Tuple3 <Integer, int[], float[]>> out) throws Exception {
					int nnz = 0;
					for (int i = 0; i < value.f2.length; i++) {
						if (value.f2[i] > 0.) {
							nnz++;
						}
					}
					int[] indices = new int[nnz];
					float[] scores = new float[nnz];
					for (int i = 0; i < nnz; i++) {
						indices[i] = (int) value.f1[i];
						scores[i] = value.f2[i];
					}
					out.collect(Tuple3.of(value.f0.intValue(), indices, scores));
				}
			});
	}

	private enum ModelType implements Serializable {
		Simrank,
		SimrankPP,
	}

	private static class NextStepOp extends RichMapFunction <
		Tuple3 <Integer, Integer, int[][]>,
		Tuple3 <Integer, Integer, int[][]>> {

		private static final long serialVersionUID = -6867878895306425045L;
		private List <Tuple2 <Integer, int[]>> nextPosBc = null;
		private int[][] nextPos = null;
		private int who;

		public NextStepOp(int who) {
			this.who = who;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			nextPosBc = getRuntimeContext().getBroadcastVariable("nextPos");
			nextPos = new int[nextPosBc.size()][0];

			for (Tuple2 <Integer, int[]> pos : nextPosBc) {
				nextPos[pos.f0] = pos.f1;
			}
		}

		@Override
		public Tuple3 <Integer, Integer, int[][]> map(Tuple3 <Integer, Integer, int[][]> value) throws Exception {
			int step = getIterationRuntimeContext().getSuperstepNumber() - 1;
			int[][] path = value.f2;
			int nodeId = value.f1;

			int N = path.length;
			int currPos = step * 2 - who;
			for (int i = 0; i < N; i++) {
				int nodePos = (currPos < 0 ? nodeId : path[i][currPos]);
				path[i][currPos + 1] = nextPos[nodePos][i];
			}

			return new Tuple3 <>(value.f0, value.f1, path);
		}
	}

	private static class NextPosOp
		extends RichMapFunction <Tuple4 <Integer, Integer, int[], float[]>, Tuple2 <Integer, int[]>> {

		private static final long serialVersionUID = -3989486592394853774L;
		String modelTypeString = null;
		ModelType modelType = null;
		Random random = null;
		int N;
		int who;

		public NextPosOp(String modelTypeString, int N, int who) {
			this.modelTypeString = modelTypeString;
			this.N = N;
			this.who = who;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			this.random = new Random(getRuntimeContext().getIndexOfThisSubtask() +
				getIterationRuntimeContext().getSuperstepNumber() * getRuntimeContext().getNumberOfParallelSubtasks()
				+ who);
			this.modelType = (modelTypeString.equalsIgnoreCase("simrank") ? ModelType.Simrank : ModelType.SimrankPP);
		}

		@Override
		public Tuple2 <Integer, int[]> map(Tuple4 <Integer, Integer, int[], float[]> node) throws Exception {
			return new Tuple2 <>(node.f1, nextPos(node.f2, node.f3));
		}

		private int[] nextPos(int[] neighbor, float[] weights) {
			int nnz = neighbor.length;
			double[] prob = new double[nnz];
			int[] next = new int[N];

			if (modelType.equals(ModelType.Simrank)) {
				for (int i = 0; i < nnz; i++) {
					double p = weights[i];
					int nb = neighbor[i];
					double spread = 1.0; // temp
					p = p * spread;
					if (i == 0) {
						prob[i] = p;
					} else {
						prob[i] = prob[i - 1] + p;
					}
				}
			} else if (modelType.equals(ModelType.SimrankPP)) {
				for (int i = 0; i < nnz; i++) {
					double p = weights[i];
					if (i == 0) {
						prob[i] = p;
					} else {
						prob[i] = prob[i - 1] + p;
					}
				}
			} else {
				throw new RuntimeException("unknown model type");
			}

			for (int n = 0; n < N; n++) {
				double r = random.nextDouble();
				int pos = Arrays.binarySearch(prob, r);
				if (pos < 0) {
					pos = -(pos + 1);
				}

				next[n] = (pos >= nnz ? -1 : neighbor[pos]);
			}
			return next;
		}
	}

	private static class NormalizeWeightsOp extends RichMapFunction <
		Tuple4 <Integer, Integer, int[], float[]>,
		Tuple4 <Integer, Integer, int[], float[]>> {
		private static final long serialVersionUID = 1383052403338418414L;
		private int N;
		private int L;

		public NormalizeWeightsOp(int N, int L) {
			this.N = N;
			this.L = L;
		}

		@Override
		public Tuple4 <Integer, Integer, int[], float[]> map(Tuple4 <Integer, Integer, int[], float[]> value)
			throws Exception {
			float max = 0.F;
			float sum = 0.F;
			for (float v : value.f3) {
				if (v > max) {
					max = v;
				}
				sum += v;
			}

			//            double aver = sum / value.f2.length;
			//            for (float v : value.f3)
			//                var += (v - aver) * (v - aver);
			//            var /= (max * max);
			//            var /= value.f2.length;
			//            float spread = ((Double) Math.exp(var)).floatValue();
			//            float[] weights = new float[value.f3.length + 1];
			//            weights[weights.length - 1] = spread;

			// normalizeEqual the weights
			float[] weights = value.f3;
			for (int i = 0; i < weights.length; i++) {
				weights[i] = value.f3[i] / sum;
			}

			return new Tuple4 <>(value.f0, value.f1, value.f2, weights);
		}
	}

	private static class SimrankSim implements Serializable {
		private static final long serialVersionUID = 4507612302560039466L;
		private final int EVIDENCE_TRUNC = 12;
		int N;
		int L;
		double C;
		double[] powerC;
		double[] evidence;
		ModelType modelType = null;
		int[] ovlRecord = new int[EVIDENCE_TRUNC];
		int[] meetCount = null;

		public SimrankSim(int n, int l, double c, String modelTypeString) {
			N = n;
			L = l;
			C = c;
			meetCount = new int[L];
			modelType = modelTypeString.equalsIgnoreCase("simrank") ? ModelType.Simrank : ModelType.SimrankPP;

			powerC = new double[L];
			for (int i = 0; i < powerC.length; i++) {
				powerC[i] = Math.pow(C, i + 1);
			}

			evidence = new double[EVIDENCE_TRUNC + 1];
			evidence[0] = 0.;
			double half = 0.5;
			for (int i = 1; i <= EVIDENCE_TRUNC; i++) {
				evidence[i] = evidence[i - 1] + half;
				half = half * 0.5;
			}
			evidence[0] = 0.5;
		}

		public double similarity(int[][] a, int[][] b) {
			if (modelType.equals(ModelType.Simrank)) {
				Arrays.fill(meetCount, 0);
				int totMeetCount = 0;
				for (int n = 0; n < N; n++) {
					for (int l = 0; l < L; l++) {
						if (a[n][l] == b[n][l]) {
							if (a[n][l] != -1) {
								meetCount[l]++;
								totMeetCount++;
							}
							break;
						}
					}
				}

				if (totMeetCount == 0) {
					return 0.F;
				} else {
					double s = 0.F;
					for (int l = 0; l < L; l++) {
						if (meetCount[l] == 0) {
							continue;
						}
						s += meetCount[l] * powerC[l];
					}
					s /= N;
					return s;
				}
			} else if (modelType.equals(ModelType.SimrankPP)) {
				int totMeetCount = 0;
				int novl = 0;
				Arrays.fill(meetCount, 0);
				for (int n = 0; n < N; n++) {
					for (int l = 0; l < L; l++) {
						if (a[n][l] == b[n][l]) {
							if (a[n][l] != -1) {
								meetCount[l]++;
								totMeetCount++;
								if (l == 0 && novl < EVIDENCE_TRUNC) {
									novl = countEvidence(ovlRecord, novl, a[n][0]);
								}
							}
							break;
						}
					}
				}

				if (totMeetCount == 0) {
					return 0.F;
				}

				double s = 0.F;
				for (int l = 0; l < L; l++) {
					if (meetCount[l] == 0) {
						continue;
					}
					s += meetCount[l] * powerC[l];
				}
				s /= N;

				return s * evidence[novl];
			} else {
				throw new RuntimeException("unexpected");
			}
		}
	}

	private static class WalkOneStepOp extends RichCoGroupFunction <
		Tuple3 <Integer, Integer, int[][]>,
		Tuple4 <Integer, Integer, Integer, int[]>,
		Tuple3 <Integer, Integer, int[][]>> {

		private static final long serialVersionUID = -8693439575813692881L;
		private int N;
		private int who;

		public WalkOneStepOp(int n, int who) {
			N = n;
			this.who = who;
		}

		@Override
		public void coGroup(Iterable <Tuple3 <Integer, Integer, int[][]>> paths, // paths: blockId, nodeId, path
							Iterable <Tuple4 <Integer, Integer, Integer, int[]>> responses,
							// responses: recvBlockId, sendBlockId, N, response
							Collector <Tuple3 <Integer, Integer, int[][]>> out) throws Exception {

			// buffer reponses
			List <Map <Integer, Integer>> nexPos = new ArrayList <>(N);
			for (int i = 0; i < N; i++) {
				nexPos.add(new HashMap <Integer, Integer>());
			}
			for (Tuple4 <Integer, Integer, Integer, int[]> response : responses) {
				int n = response.f2;
				int cnt = response.f3.length / 2;
				for (int i = 0; i < cnt; i++) {
					int nodeId = response.f3[i * 2 + 0];
					int nextId = response.f3[i * 2 + 1];
					nexPos.get(n).put(nodeId, nextId);
				}
			}

			int step = getIterationRuntimeContext().getSuperstepNumber() - 1;
			int currPos = step * 2 - who;

			// step forward
			for (Tuple3 <Integer, Integer, int[][]> path : paths) {
				int nodeId = path.f1;
				int[][] fp = path.f2;
				for (int i = 0; i < N; i++) {
					int nodePos = (currPos < 0 ? nodeId : fp[i][currPos]);
					fp[i][currPos + 1] = nexPos.get(i).get(nodePos);
				}
				out.collect(new Tuple3 <>(path.f0, path.f1, fp));
			}
		}
	}

	private static class AppendIdToPathOp
		extends RichMapFunction <Tuple2 <Integer, int[]>, Tuple3 <Integer, Integer, int[]>> {
		private static final long serialVersionUID = -9008691431256443766L;
		private List <Tuple3 <Integer, Integer, Long>> graphCounts = null;
		private int who;
		private int numBlocks;
		private int[] srcStarts = null;
		private int[] srcCounts = null;

		public AppendIdToPathOp(int who, int numBlocks) {
			this.numBlocks = numBlocks;
			this.who = who;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			this.graphCounts = getRuntimeContext().getBroadcastVariable("graphCounts");

			int numSrcs = graphCounts.get(0).getField(who);
			this.srcStarts = GraphUtils.BlockPartitioner.getBlockStarts(numBlocks, numSrcs);
			this.srcCounts = GraphUtils.BlockPartitioner.getBlockCounts(numBlocks, numSrcs);
		}

		@Override
		public Tuple3 <Integer, Integer, int[]> map(Tuple2 <Integer, int[]> value) throws Exception {
			return new Tuple3 <>(GraphUtils.BlockPartitioner.getBlockId(srcStarts, value.f0), value.f0, value.f1);
		}
	}

	private static class GenerateResponseOp extends RichCoGroupFunction <
		Tuple3 <Integer, Integer, int[]>,  // nextPosWithBlockId: senderBlockId, senderNodeId, nextPos
		Tuple4 <Integer, Integer, Integer, int[]>, // request: recvBlockId, senderBlockId, N, request (ordered by N)
		Tuple4 <Integer, Integer, Integer, int[]>> // response: recvBlockId, senderBlockId, N, response
	{

		private static final long serialVersionUID = 1317147465654535129L;

		@Override
		public void coGroup(Iterable <Tuple3 <Integer, Integer, int[]>> sender,
							Iterable <Tuple4 <Integer, Integer, Integer, int[]>> requests,
							Collector <Tuple4 <Integer, Integer, Integer, int[]>> responses) throws Exception {

			if (null == requests) {
				return;
			}

			// buffer the sender(nextPosOwner)
			int startIdx = Integer.MAX_VALUE;
			int endIdx = 0;
			List <Tuple3 <Integer, Integer, int[]>> nextPosBuffer = new ArrayList <>();
			for (Tuple3 <Integer, Integer, int[]> s : sender) {
				nextPosBuffer.add(s);
				startIdx = Math.min(startIdx, s.f1);
				endIdx = Math.max(endIdx, s.f1);
			}

			// handle the request
			for (Tuple4 <Integer, Integer, Integer, int[]> request : requests) {
				int[] req = request.f3;
				int n = request.f2;
				int num = req.length;
				int[] response = new int[req.length * 2];

				for (int i = 0; i < num; i++) {
					int reqNodeId = req[i];
					if (reqNodeId < startIdx || reqNodeId > endIdx) {
						throw new RuntimeException("unexpected");
					}
					int nextPos = nextPosBuffer.get(reqNodeId - startIdx).f2[n];
					response[i * 2 + 0] = req[i];
					response[i * 2 + 1] = nextPos;
				}
				responses.collect(new Tuple4 <>(request.f0, request.f1, request.f2, response));
			}

		}
	}

	private static class GenerateRequestOp extends RichGroupReduceFunction <
		Tuple3 <Integer, Integer, int[][]>,
		Tuple4 <Integer, Integer, Integer, int[]>> {

		private static final long serialVersionUID = -4242001917484011208L;
		int N;
		int sender;
		private List <Tuple3 <Integer, Integer, Long>> graphCounts = null;
		private int numBlocks;
		private int[] senderStarts = null;
		private int[] senderCounts = null;

		public GenerateRequestOp(int sender, int numBlocks, int N) {
			this.sender = sender;
			this.numBlocks = numBlocks;
			this.N = N;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			this.graphCounts = getRuntimeContext().getBroadcastVariable("graphCounts");
		}

		@Override
		public void reduce(Iterable <Tuple3 <Integer, Integer, int[][]>> currentPath,
						   Collector <Tuple4 <Integer, Integer, Integer, int[]>> out) throws Exception {

			int step = getIterationRuntimeContext().getSuperstepNumber() - 1;

			int numSrcs = graphCounts.get(0).getField(sender);
			this.senderStarts = GraphUtils.BlockPartitioner.getBlockStarts(numBlocks, numSrcs);
			this.senderCounts = GraphUtils.BlockPartitioner.getBlockCounts(numBlocks, numSrcs);

			int dstBlockId = -1;
			List <Tuple3 <Integer, Integer, int[][]>> buffer = new ArrayList <>();
			for (Tuple3 <Integer, Integer, int[][]> v : currentPath) {
				dstBlockId = v.f0;
				buffer.add(v);
			}

			int currPos = step * 2 - sender;

			for (int srcBlockId = 0; srcBlockId < numBlocks; srcBlockId++) {
				List <Set <Integer>> whatIsNeed = new ArrayList <>(N);
				for (int i = 0; i < N; i++) {
					whatIsNeed.add(null);
				}

				for (Tuple3 <Integer, Integer, int[][]> dstNode : buffer) {
					int nodeId = dstNode.f1; // dst node
					int[][] path = dstNode.f2;
					for (int i = 0; i < N; i++) {
						int nodePos = (currPos < 0 ? nodeId : path[i][currPos]);
						int senderBlockId = GraphUtils.BlockPartitioner.getBlockId(senderStarts, nodePos);
						if (senderBlockId != srcBlockId) {
							continue;
						}
						if (whatIsNeed.get(i) == null) {
							whatIsNeed.set(i, new HashSet <Integer>());
						}
						whatIsNeed.get(i).add(nodePos);
					}
				}

				for (int i = 0; i < N; i++) {
					if (whatIsNeed.get(i) != null) {
						Set <Integer> reqNodes = whatIsNeed.get(i);
						int[] req = new int[reqNodes.size()];
						int pos = 0;
						for (Integer r : reqNodes) {
							req[pos++] = r;
						}
						out.collect(new Tuple4 <>(dstBlockId, srcBlockId, i, req));
					}
				}
			}
		}
	}
}
