package org.apache.rocketmq.streams.rstream;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.rocketmq.streams.function.FilterAction;
import org.apache.rocketmq.streams.function.ForeachAction;
import org.apache.rocketmq.streams.function.KeySelectAction;
import org.apache.rocketmq.streams.function.ValueMapperAction;
import org.apache.rocketmq.streams.serialization.KeyValueSerializer;
import org.apache.rocketmq.streams.serialization.Serializer;

public interface RStream<T> {

    <O> RStream<O> map(ValueMapperAction<T, O> mapperAction);

    <VR> RStream<T> flatMapValues(final ValueMapperAction<? extends T, ? extends Iterable<? extends VR>> mapper);

    RStream<T> filter(FilterAction<T> predictor);

    <K> GroupedStream<K,T> keyBy(KeySelectAction<K, T> keySelectAction);

    void print();

    void foreach(ForeachAction<T> foreachAction);

    <K> void sink(String topicName, KeyValueSerializer<K, T> serializer);
}
