package com.jayway.perfectstorm.storm.bolt.countrycount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Map;

import static backtype.storm.utils.Utils.tuple;
import static com.jayway.restassured.RestAssured.get;

public class GeolocationToCountryNameBolt extends BaseRichBolt {

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        final double latitude = tuple.getDouble(0);
        final double longitude = tuple.getDouble(1);

        final String countryName = get("http://ws.geonames.org/findNearbyPlaceName?lat={lat}&lng={long}", latitude, longitude).xmlPath().getString("geonames.geoname.countryName");

        outputCollector.emit(tuple(countryName));

        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("country"));
    }
}
