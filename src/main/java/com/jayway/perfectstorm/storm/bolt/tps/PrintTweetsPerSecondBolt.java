package com.jayway.perfectstorm.storm.bolt.tps;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

public class PrintTweetsPerSecondBolt extends BaseRichBolt {

    private transient OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        final Long tps = tuple.getLong(0);
        System.out.printf("Tweets per second: %d\n", tps);
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
