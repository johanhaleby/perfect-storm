package com.jayway.perfectstorm.storm.bolt.tweetfind;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;

import java.util.HashMap;
import java.util.Map;

public class FoundTweetsPublisherBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    private transient HazelcastInstance hazelcast;
    private transient IQueue<Object> queue;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        hazelcast = Hazelcast.newHazelcastInstance();
        queue = hazelcast.getQueue("found-tweets");
    }

    @Override
    public void execute(Tuple tuple) {
        final String tweet = tuple.getString(0);
        final String author = tuple.getString(1);

        queue.offer(buildEvent(tweet, author));

        outputCollector.ack(tuple);
    }

    private Map<String, Object> buildEvent(String tweet, String author) {
        Map<String, Object> event = new HashMap<>();

        Map<String, Object> values = new HashMap<>();
        values.put("author", author);
        values.put("tweet", tweet);
        event.put("eventName", "matching-tweets");
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
