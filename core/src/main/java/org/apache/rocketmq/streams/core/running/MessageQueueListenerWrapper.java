package org.apache.rocketmq.streams.core.running;
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

import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.core.topology.TopologyBuilder;

import java.util.HashMap;
import java.util.Set;

class MessageQueueListenerWrapper implements MessageQueueListener {
    private static final String pattern = "%s@%s";
    private final MessageQueueListener originListener;
    private final TopologyBuilder topologyBuilder;

    private final HashMap<String, Processor<?>> mq2Processor = new HashMap<>();


    MessageQueueListenerWrapper(MessageQueueListener originListener, TopologyBuilder topologyBuilder) {
        this.originListener = originListener;
        this.topologyBuilder = topologyBuilder;
    }

    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
        try {
            //todo 构建拓扑图，取得task
            buildTask(topic, mqDivided);

        } catch (Throwable e) {
            e.printStackTrace();
        }

        //在构建好执行processor之后，才执行原始listener
        originListener.messageQueueChanged(topic, mqAll, mqDivided);
    }

    //todo 本实例消费的所有的q都重新build了一遍，但是存在前后两次重平衡消费不变的q，不需要build得到新的processor了
    private <T> void buildTask(String topicName, Set<MessageQueue> mqDivided) {
        for (MessageQueue messageQueue : mqDivided) {
            Processor<T> processor = topologyBuilder.build(topicName);
            String key = buildKey(messageQueue.getTopic(), messageQueue.getQueueId());
            //todo 不同的q使用的processor为什么是一样的？不同的q应该使用不一样的processor实例。
            this.mq2Processor.put(key, processor);
        }
    }

    @SuppressWarnings("unchecked")
    <T> Processor<T> selectProcessor(String key) {
//        String key = buildKey(topic, queueId);
        return (Processor<T>) this.mq2Processor.get(key);
    }

    String buildKey(String topic, int queueId) {
        return String.format(pattern, topic, queueId);
    }
}
