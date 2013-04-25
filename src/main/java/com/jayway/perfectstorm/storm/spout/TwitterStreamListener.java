package com.jayway.perfectstorm.storm.spout;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

import java.util.concurrent.BlockingQueue;

class TwitterStreamListener implements StatusListener {

    private final BlockingQueue<Status> tweetQueue;

    public TwitterStreamListener(BlockingQueue<Status> tweetQueue) {
        this.tweetQueue = tweetQueue;
    }

    @Override
    public void onStatus(Status status) {
        tweetQueue.offer(status);
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {
    }

    @Override
    public void onStallWarning(StallWarning warning) {
    }

    @Override
    public void onException(Exception ex) {
    }
}
