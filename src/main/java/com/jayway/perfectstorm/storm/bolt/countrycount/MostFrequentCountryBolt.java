package com.jayway.perfectstorm.storm.bolt.countrycount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.jayway.perfectstorm.esper.EsperConfig;
import com.jayway.perfectstorm.esper.EsperContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static backtype.storm.utils.Utils.tuple;
import static com.jayway.perfectstorm.storm.bolt.countrycount.MostFrequentCountryBolt.MostFrequentCountryEsperConfig.*;
import static java.lang.String.format;

public class MostFrequentCountryBolt extends BaseRichBolt implements UpdateListener {

    public static class MostFrequentCountryEsperConfig extends EsperConfig {
        static final String EVENT_TYPE_NAME = "Countries";

        static final String NUMBER_OF_TWEETS_FROM_COUNTRY = "noOfTweetsFromCountry";
        static final String COUNTRY_OUTPUT_NAME = "countryName";
        static final String COUNTRY_INPUT_NAME = "country";
        // http://esper.codehaus.org/esper-4.2.0/doc/reference/en/html/epl_clauses.html
        static final String EPL = format(
                "select count(*) as %s, %s as %s from %s.win:time_batch(10 sec) group by country order by %s desc",
                NUMBER_OF_TWEETS_FROM_COUNTRY, COUNTRY_INPUT_NAME, COUNTRY_OUTPUT_NAME, EVENT_TYPE_NAME, NUMBER_OF_TWEETS_FROM_COUNTRY
        );

        public MostFrequentCountryEsperConfig() {
            super(EPL, EVENT_TYPE_NAME, NUMBER_OF_TWEETS_FROM_COUNTRY, COUNTRY_OUTPUT_NAME, COUNTRY_INPUT_NAME);
        }
    }

    private transient OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        EsperContext.addListener(this, EPL);
    }

    @Override
    public void execute(Tuple tuple) {
        final String countryName = tuple.getString(0);

        Map<String, Object> data = new HashMap<>();
        data.put(COUNTRY_INPUT_NAME, countryName);

        EsperContext.sendEvent(data, EVENT_TYPE_NAME);

        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("number-country", new Fields(NUMBER_OF_TWEETS_FROM_COUNTRY, COUNTRY_OUTPUT_NAME));
        outputFieldsDeclarer.declareStream("country-batch", new Fields("country-batch"));
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (newEvents == null) {
            return;
        }

        List<Map<String, Object>> batch = new ArrayList<>();
        for (EventBean event : newEvents) {
            final Object numberOfTweetsForCountry = event.get(NUMBER_OF_TWEETS_FROM_COUNTRY);
            final String countryName = (String) event.get(COUNTRY_OUTPUT_NAME);
            final Map<String, Object> map = new HashMap<>();
            map.put("countryName", countryName);
            map.put("tweetCount", numberOfTweetsForCountry);
            batch.add(map);
            outputCollector.emit("number-country", tuple(numberOfTweetsForCountry, countryName));
        }
        outputCollector.emit("country-batch", tuple(batch));
    }
}
