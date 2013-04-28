package com.jayway.perfectstorm.storm.bolt.countrycount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.jayway.perfectstorm.esper.EsperContext;

import java.util.HashMap;
import java.util.Map;

import static backtype.storm.utils.Utils.tuple;
import static java.lang.String.format;

public class MostFrequentCountryBolt extends BaseRichBolt implements UpdateListener {
    private static final String EVENT_TYPE_NAME = "Countries";

    private static final String NUMBER_OF_TWEETS_FROM_COUNTRY = "noOfTweetsFromCountry";
    private static final String COUNTRY_OUTPUT_NAME = "countryName";
    private static final String COUNTRY_INPUT_NAME = "country";
    // http://esper.codehaus.org/esper-4.2.0/doc/reference/en/html/epl_clauses.html
    private static final String ESPER_QUERY = format(
            "select count(*) as %s, %s as %s from %s.win:time_batch(10 sec) group by country order by %s desc",
            NUMBER_OF_TWEETS_FROM_COUNTRY, COUNTRY_INPUT_NAME, COUNTRY_OUTPUT_NAME, EVENT_TYPE_NAME, NUMBER_OF_TWEETS_FROM_COUNTRY
    );


    private transient OutputCollector outputCollector;
    private transient EsperContext esperContext;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        esperContext = EsperContext.create(this, ESPER_QUERY, EVENT_TYPE_NAME, NUMBER_OF_TWEETS_FROM_COUNTRY, COUNTRY_OUTPUT_NAME, COUNTRY_INPUT_NAME);
    }

    @Override
    public void execute(Tuple tuple) {
        final String countryName = tuple.getString(0);

        Map<String, Object> data = new HashMap<>();
        data.put(COUNTRY_INPUT_NAME, countryName);

        esperContext.sendEvent(data);

        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(NUMBER_OF_TWEETS_FROM_COUNTRY, COUNTRY_OUTPUT_NAME));
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (newEvents == null) {
            return;
        }
        for (EventBean event : newEvents) {
            final Object numberOfTweetsForCountry = event.get(NUMBER_OF_TWEETS_FROM_COUNTRY);
            final Object countryName = event.get(COUNTRY_OUTPUT_NAME);
            outputCollector.emit(tuple(numberOfTweetsForCountry, countryName));
        }
    }


    @Override
    public void cleanup() {
        if (esperContext != null) {
            esperContext.shutdown();
        }
    }
}
