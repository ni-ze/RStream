package org.apache.rocketmq.streams.source;

public interface SourceFunction<T> {
    void run(SourceContext<T> ctx) throws Exception;


    interface SourceContext<T> {
        void collect(T element);
    }
}
