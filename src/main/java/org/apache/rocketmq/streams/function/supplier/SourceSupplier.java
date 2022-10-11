package org.apache.rocketmq.streams.function.supplier;
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

import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.streams.common.Constant;
import org.apache.rocketmq.streams.metadata.Context;
import org.apache.rocketmq.streams.running.AbstractProcessor;
import org.apache.rocketmq.streams.running.Processor;
import org.apache.rocketmq.streams.running.StreamContext;
import org.apache.rocketmq.streams.serialization.Deserializer;
import org.apache.rocketmq.streams.serialization.KeyValueDeserializer;

import java.util.function.Supplier;

public class SourceSupplier<K,V> implements Supplier<Processor<V>> {
    private String topicName;
    private KeyValueDeserializer<K, V> deserializer;

    public SourceSupplier(String topicName, KeyValueDeserializer<K, V> deserializer) {
        this.topicName = topicName;
        this.deserializer = deserializer;
    }

    @Override
    public Processor<V> get() {
        return new SourceProcessorImpl(deserializer);
    }

    public interface SourceProcessor<K, V> extends Processor<V> {
        Pair<K, V> deserialize(byte[] data) throws Throwable;
    }

    private class SourceProcessorImpl extends AbstractProcessor<V> implements SourceProcessor<K,V> {
        private KeyValueDeserializer<K, V> deserializer;


        public SourceProcessorImpl(KeyValueDeserializer<K, V> deserializer) {
            this.deserializer = deserializer;
        }

        @Override
        public void preProcess(StreamContext<V> context) throws Throwable {
            super.preProcess(context);
            this.deserializer.configure(context.getAdditional().get(Constant.SHUFFLE_KEY_CLASS_NAME), context.getAdditional().get(Constant.SHUFFLE_VALUE_CLASS_NAME));
        }

        @Override
        public Pair<K, V> deserialize(byte[] data) throws Throwable {
            return this.deserializer.deserialize(data);
        }

        @Override
        public void process(V data) throws Throwable {
            Context<K, V> result = new Context<>(this.context.getKey(), data);
            this.context.forward(result);
        }
    }
}
