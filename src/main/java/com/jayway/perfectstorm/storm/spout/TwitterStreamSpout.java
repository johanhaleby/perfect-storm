package com.jayway.perfectstorm.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static backtype.storm.utils.Utils.tuple;

public class TwitterStreamSpout extends BaseRichSpout {

    private final String username;
    private final String password;
    private transient SpoutOutputCollector collector;
    private transient BlockingQueue<Status> tweetQueue;
    private transient TwitterStream twitterStream;

    public TwitterStreamSpout(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("tweet-stream", new Fields("tweet", "author"));
        outputFieldsDeclarer.declareStream("tweet-geo", new Fields("lat", "long"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
        tweetQueue = new ArrayBlockingQueue<>(3000);

        Configuration twitterConf = new ConfigurationBuilder().setUser(username).setPassword(password).build();
        TwitterStreamFactory factory = new TwitterStreamFactory(twitterConf);
        twitterStream = factory.getInstance();
        twitterStream.addListener(new TwitterStreamListener(tweetQueue));

        twitterStream.sample();
    }

    @Override
    public void nextTuple() {
        Status tweet = tweetQueue.poll();
        if (tweet == null) {
            Utils.sleep(100);
            return;
        }

        final GeoLocation geoLocation = tweet.getGeoLocation();
        if (geoLocation != null) {
            final double latitude = geoLocation.getLatitude();
            final double longitude = geoLocation.getLongitude();
            collector.emit("tweet-geo", tuple(latitude, longitude));
        }

        collector.emit("tweet-stream", tuple(tweet.getText(), tweet.getUser().getName()));
    }

    @Override
    public void close() {
        twitterStream.shutdown();
    }
}
