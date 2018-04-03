/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.keyed.state.kryo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 *
 */
public class KeyedStateKryoTestProgram {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.registerTypeWithKryoSerializer(DummyEntity.class, DummyEntitySerializer.class);
		env.getConfig().setParallelism(1);

		DummyEntity dummyEntity1 = new DummyEntity("address1", "name1", 23);
		DummyEntity dummyEntity2 = new DummyEntity("address2", "name2", 27);
		DummyEntity dummyEntity3 = new DummyEntity("address3", "name3", 21);
		DummyEntity dummyEntity4 = new DummyEntity("address2", "name2", 21);
		DummyEntity dummyEntity5 = new DummyEntity("address2", "name4", 21);
		DummyEntity dummyEntity6 = new DummyEntity("address8", "name7", 21);
		DummyEntity dummyEntity7 = new DummyEntity("address8", "name7", 29);

		DataStream<Tuple2<String, Integer>> dataStream = env
			.fromElements(dummyEntity1, dummyEntity2, dummyEntity3, dummyEntity4, dummyEntity5, dummyEntity6, dummyEntity7)
			.flatMap(new EntityProcessFunction())
			// here we can use address as a key for grouping.
			.keyBy(0)
			.sum(1);

		dataStream.print();
		env.execute();
	}

	public static class EntityProcessFunction implements FlatMapFunction<DummyEntity, Tuple2<String, Integer>> {
		@Override
		public void flatMap(DummyEntity dummyEntity, Collector<Tuple2<String, Integer>> out) throws Exception {
				out.collect(new Tuple2<String, Integer>(dummyEntity.getAddress(), 1));
		}
	}
}
