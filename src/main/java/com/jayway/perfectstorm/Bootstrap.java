package com.jayway.perfectstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.jayway.perfectstorm.esper.EsperContext;
import com.jayway.perfectstorm.storm.bolt.countrycount.GeolocationToCountryNameBolt;
import com.jayway.perfectstorm.storm.bolt.countrycount.MostFrequentCountryBolt;
import com.jayway.perfectstorm.storm.bolt.countrycount.MostFrequentCountryPresenterBolt;
import com.jayway.perfectstorm.storm.bolt.countrycount.MostFrequentCountryPublisherBolt;
import com.jayway.perfectstorm.storm.bolt.tps.CalculateTweetsPerSecondBolt;
import com.jayway.perfectstorm.storm.bolt.tps.PrintTweetsPerSecondBolt;
import com.jayway.perfectstorm.storm.bolt.tweetfind.FindTweetContainingStringBolt;
import com.jayway.perfectstorm.storm.bolt.tweetfind.FoundTweetsPublisherBolt;
import com.jayway.perfectstorm.storm.spout.TwitterStreamSpout;
import com.jayway.perfectstorm.vertx.VertxServer;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import static com.jayway.perfectstorm.storm.bolt.countrycount.MostFrequentCountryBolt.MostFrequentCountryEsperConfig;
import static com.jayway.perfectstorm.storm.bolt.tps.CalculateTweetsPerSecondBolt.CalculateTweetsPerSecondEsperConfig;

public class Bootstrap {

    public static void main(String[] args) throws Exception {
        final String username = args[0];
        final String password = args[1];

        TopologyBuilder builder = new TopologyBuilder();

        // Vertx
        VertxServer vertxServer = new VertxServer();
        vertxServer.start();

        // Esper
        EsperContext.initializeWith(new MostFrequentCountryEsperConfig(), new CalculateTweetsPerSecondEsperConfig());


        // Storm

        // Twitter country count
        TwitterStreamSpout twitterStreamSpout = new TwitterStreamSpout(username, password);
        GeolocationToCountryNameBolt geolocationToCountryNameBolt = new GeolocationToCountryNameBolt();
        MostFrequentCountryBolt mostFrequentCountryBolt = new MostFrequentCountryBolt();
        MostFrequentCountryPresenterBolt mostFrequentCountryPresenterBolt = new MostFrequentCountryPresenterBolt();
        MostFrequentCountryPublisherBolt mostFrequentCountryPublisherBolt = new MostFrequentCountryPublisherBolt();

        // Tweets per second
        CalculateTweetsPerSecondBolt calculateTweetsPerSecondBolt = new CalculateTweetsPerSecondBolt();
        PrintTweetsPerSecondBolt printTweetsPerSecondBolt = new PrintTweetsPerSecondBolt();

        // Tweets containing string
        FindTweetContainingStringBolt findTweetContainingStringBolt = new FindTweetContainingStringBolt();
        FoundTweetsPublisherBolt foundTweetsPublisherBolt = new FoundTweetsPublisherBolt();


        builder.setSpout("twitter-stream", twitterStreamSpout);

        builder.setBolt("location-to-country", geolocationToCountryNameBolt, 2).shuffleGrouping("twitter-stream", "tweet-geo");
        builder.setBolt("country-frequency", mostFrequentCountryBolt).shuffleGrouping("location-to-country");
        builder.setBolt("presenter", mostFrequentCountryPresenterBolt).shuffleGrouping("country-frequency", "number-country");
        builder.setBolt("publisher", mostFrequentCountryPublisherBolt).shuffleGrouping("country-frequency", "country-batch");

        builder.setBolt("tps-calc", calculateTweetsPerSecondBolt).shuffleGrouping("twitter-stream", "tweet-stream");
        builder.setBolt("tps-print", printTweetsPerSecondBolt).shuffleGrouping("tps-calc");

        builder.setBolt("tweet-containing-string", findTweetContainingStringBolt).shuffleGrouping("twitter-stream", "tweet-stream");
        builder.setBolt("found-tweet-publisher", foundTweetsPublisherBolt).shuffleGrouping("tweet-containing-string");

        Config conf = new Config();
        conf.setDebug(false);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("twitter-test", conf, builder.createTopology());


        // Wait until 'quit'
        System.out.println("Write 'quit' to quit");
        InputStreamReader converter = new InputStreamReader(System.in);
        BufferedReader in = new BufferedReader(converter);
        String command = "";
        while (!(command.equals("quit"))) {
            command = in.readLine();
            if (command.equals("quit")) {
                break;
            }
        }

        // Shutdown everything
        cluster.killTopology("twitter-test");
        cluster.shutdown();
        EsperContext.shutdown();
        vertxServer.stop();
    }
}
