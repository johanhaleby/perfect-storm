package com.jayway.perfectstorm.storm.bolt.countrycount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.espertech.esper.client.*;

import java.util.HashMap;
import java.util.Map;

import static backtype.storm.utils.Utils.tuple;
import static java.lang.String.format;

public class MostFrequentCountryBolt extends BaseRichBolt implements UpdateListener {

    private static final String EVENT_TYPE_NAME = "Countries";
    private static final String NUMBER_OF_TWEETS_FROM_COUNTRY = "noOfTweetsFromCountry";
    private static final String COUNTRY_OUTPUT_NAME = "countryName";
    private static final String COUNTRY_INPUT_NAME = "country";
    private static final String ESPER_QUERY = format(
            "select count(*) as %s, %s as %s from %s.win:time_batch(10 sec) group by country order by %s desc",
            NUMBER_OF_TWEETS_FROM_COUNTRY, COUNTRY_INPUT_NAME, COUNTRY_OUTPUT_NAME, EVENT_TYPE_NAME, NUMBER_OF_TWEETS_FROM_COUNTRY
    );

    private transient OutputCollector outputCollector;
    private transient EPServiceProvider esperProvider;
    private transient EPRuntime epRuntime;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        configureEsper();
    }

    private void configureEsper() {
        Configuration configuration = new Configuration();
        Map<String, Object> props = new HashMap<>();
        props.put(NUMBER_OF_TWEETS_FROM_COUNTRY, Object.class);
        props.put(COUNTRY_INPUT_NAME, Object.class);
        props.put(COUNTRY_OUTPUT_NAME, Object.class);
        configuration.addEventType(EVENT_TYPE_NAME, props);

        esperProvider = EPServiceProviderManager.getProvider(null, configuration);
        esperProvider.initialize();
        epRuntime = esperProvider.getEPRuntime();
        EPAdministrator esperAdmin = esperProvider.getEPAdministrator();

        EPStatement statement = esperAdmin.createEPL(ESPER_QUERY);
        statement.addListener(this);
    }

    @Override
    public void execute(Tuple tuple) {
        final String countryName = tuple.getString(0);

        Map<String, Object> data = new HashMap<>();
        data.put(COUNTRY_INPUT_NAME, countryName);

        epRuntime.sendEvent(data, EVENT_TYPE_NAME);

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
        if (esperProvider != null) {
            esperProvider.destroy();
        }
    }
}
