package com.jayway.perfectstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import com.jayway.perfectstorm.esper.EsperContext;
import com.jayway.perfectstorm.storm.bolt.countrycount.GeolocationToCountryNameBolt;
import com.jayway.perfectstorm.storm.bolt.countrycount.MostFrequentCountryBolt;
import com.jayway.perfectstorm.storm.bolt.countrycount.MostFrequentCountryPresenterBolt;
import com.jayway.perfectstorm.storm.bolt.tps.CalculateTweetsPerSecondBolt;
import com.jayway.perfectstorm.storm.bolt.tps.PrintTweetsPerSecondBolt;
import com.jayway.perfectstorm.storm.spout.TwitterStreamSpout;

import static com.jayway.perfectstorm.storm.bolt.countrycount.MostFrequentCountryBolt.MostFrequentCountryEsperConfig;
import static com.jayway.perfectstorm.storm.bolt.tps.CalculateTweetsPerSecondBolt.CalculateTweetsPerSecondEsperConfig;

public class Bootstrap {

    public static void main(String[] args) {
        final String username = args[0];
        final String password = args[1];

        TopologyBuilder builder = new TopologyBuilder();

        // Esper
        EsperContext.initializeWith(new MostFrequentCountryEsperConfig(), new CalculateTweetsPerSecondEsperConfig());

        // Twitter country count
        TwitterStreamSpout twitterStreamSpout = new TwitterStreamSpout(username, password);
        GeolocationToCountryNameBolt geolocationToCountryNameBolt = new GeolocationToCountryNameBolt();
        MostFrequentCountryBolt mostFrequentCountryBolt = new MostFrequentCountryBolt();
        MostFrequentCountryPresenterBolt mostFrequentCountryPresenterBolt = new MostFrequentCountryPresenterBolt();

        // Tweets per second
        CalculateTweetsPerSecondBolt calculateTweetsPerSecondBolt = new CalculateTweetsPerSecondBolt();
        PrintTweetsPerSecondBolt printTweetsPerSecondBolt = new PrintTweetsPerSecondBolt();


        builder.setSpout("twitter-stream", twitterStreamSpout);

        builder.setBolt("location-to-country", geolocationToCountryNameBolt, 2).shuffleGrouping("twitter-stream", "tweet-geo");
        builder.setBolt("country-frequency", mostFrequentCountryBolt).shuffleGrouping("location-to-country");
        builder.setBolt("presenter", mostFrequentCountryPresenterBolt).shuffleGrouping("country-frequency");

        builder.setBolt("tps-calc", calculateTweetsPerSecondBolt).shuffleGrouping("twitter-stream", "tweet-stream");
        builder.setBolt("tps-print", printTweetsPerSecondBolt).shuffleGrouping("tps-calc");

        Config conf = new Config();
        conf.setDebug(false);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("twitter-test", conf, builder.createTopology());
        Utils.sleep(120000);
        cluster.shutdown();
        EsperContext.shutdown();
    }
}
