package com.jayway.perfectstorm.storm.bolt.tweetfind;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import org.apache.commons.lang.StringUtils;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static backtype.storm.utils.Utils.tuple;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FindTweetContainingStringBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    private transient volatile String wordToLookFor;
    private transient HazelcastInstance hazelcast;
    private transient IQueue<String> queue;
    private transient ScheduledExecutorService executor;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        hazelcast = Hazelcast.newHazelcastInstance();
        queue = hazelcast.getQueue("tweet-word");
        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(new WordChecker(), 1000, 200, MILLISECONDS);
    }

    @Override
    public void execute(Tuple tuple) {
        final String tweet = tuple.getString(0);
        final String author = tuple.getString(1);
        final String image = tuple.getString(2);

        if (!StringUtils.isBlank(wordToLookFor) && StringUtils.containsIgnoreCase(tweet, wordToLookFor)) {
            System.out.printf("### Found tweet containing word '%s' (Tweet: '%s' by '%s')\n", wordToLookFor, tweet, author);
            outputCollector.emit(tuple(tweet, author, image));
        }

        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet", "author", "image"));
    }

    @Override
    public void cleanup() {
        if (hazelcast != null) {
            hazelcast.getLifecycleService().shutdown();
        }
    }

    private class WordChecker implements Runnable {

        @Override
        public void run() {
            final String word = queue.poll();
            if (word == null) {
                return;
            }
            wordToLookFor = word;
        }
    }
}
