package com.jayway.perfectstorm.storm.bolt.tweetcount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

public class FindTweetContainingStringBolt extends BaseRichBolt {


    private final String wordToLookFor;
    private OutputCollector outputCollector;

    public FindTweetContainingStringBolt(String wordToLookFor) {
        this.wordToLookFor = wordToLookFor;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        final String tweet = tuple.getString(0);
        final String author = tuple.getString(1);
        if (StringUtils.containsIgnoreCase(tweet, wordToLookFor)) {
            System.out.printf("### Found tweet containing word '%s' (Tweet: '%s' by '%s')\n", wordToLookFor, tweet, author);
        }

        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
