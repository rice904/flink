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


package test;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 *  Sum up according to address. The same addresses will be grouped.
 */
public class KryoTesting {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.registerTypeWithKryoSerializer(Student.class, StudentSerializer.class);

		Student student1 =  new Student("beijing", "lisi", 23);
		Student student2 =  new Student("shanghai", "wangwu", 27);
		Student student3 =  new Student("shanghai", "stephn", 21);
		Student student4 =  new Student("shanghai", "stephn", 21);
		Student student5 =  new Student("nanjing", "stephn", 21);
		Student student6 =  new Student("shanghai", "Hurry", 21);
		Student student7 =  new Student("beijing", "lisiqw", 29);


		DataStream<Tuple2<String, Integer>> dataStream = env
			.fromElements(student1, student2, student3, student4, student5, student6, student7)
			.flatMap(new StudentProcess())
			.keyBy(0)
			.sum(1);

		dataStream.print();
		env.execute();

	}

	public static class StudentProcess implements FlatMapFunction<Student, Tuple2<String, Integer>> {
		@Override
		public void flatMap(Student stu, Collector<Tuple2<String, Integer>> out) throws Exception {
				out.collect(new Tuple2<String, Integer>(stu.getAddress(), 1));
		}
	}
}
