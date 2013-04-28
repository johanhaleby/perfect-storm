package com.jayway.perfectstorm.storm.bolt.countrycount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MostFrequentCountryPublisherBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    private transient HazelcastInstance hazelcast;
    private transient IQueue<Object> queue;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        hazelcast = Hazelcast.newHazelcastInstance();
        queue = hazelcast.getQueue("country-frequency");
    }

    @Override
    public void execute(Tuple tuple) {
        final List<Map<String, Object>> values = (List<Map<String, Object>>) tuple.getValue(0);

        queue.offer(buildEvent(values));

        outputCollector.ack(tuple);
    }

    private Map<String, Object> buildEvent(List<Map<String, Object>> values) {
        Map<String, Object> event = new HashMap<>();
        event.put("eventName", "country-frequency");
        event.put("data", values);
        return event;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void cleanup() {
        if (hazelcast != null) {
            hazelcast.getLifecycleService().shutdown();
        }
    }

}
