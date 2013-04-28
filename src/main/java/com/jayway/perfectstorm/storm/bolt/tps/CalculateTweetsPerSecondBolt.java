package com.jayway.perfectstorm.storm.bolt.tps;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.jayway.perfectstorm.esper.EsperConfig;
import com.jayway.perfectstorm.esper.EsperContext;

import java.util.HashMap;
import java.util.Map;

import static com.jayway.perfectstorm.storm.bolt.tps.CalculateTweetsPerSecondBolt.CalculateTweetsPerSecondEsperConfig.*;
import static java.lang.String.format;

public class CalculateTweetsPerSecondBolt extends BaseRichBolt implements UpdateListener {

    public static class CalculateTweetsPerSecondEsperConfig extends EsperConfig {
        static final String EVENT_TYPE_NAME = "TPS";
        static final String EPL = format("select count(*) as tps from %s.win:time_batch(1 sec)", EVENT_TYPE_NAME);
        static final String TWEET = "tweet";

        public CalculateTweetsPerSecondEsperConfig() {
            super(EPL, EVENT_TYPE_NAME, TWEET);
        }
    }

    private transient OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        EsperContext.addListener(this, EPL);
    }

    @Override
    public void execute(Tuple tuple) {
        final Map<String, Object> data = new HashMap<>();
        data.put(TWEET, 1);
        EsperContext.sendEvent(data, EVENT_TYPE_NAME);
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tps"));
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (newEvents == null) {
            return;
        }

        for (EventBean newEvent : newEvents) {
            outputCollector.emit(Utils.tuple(newEvent.get("tps")));
        }
    }
}
