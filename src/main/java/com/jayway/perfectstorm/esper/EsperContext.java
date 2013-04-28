package com.jayway.perfectstorm.esper;

import com.espertech.esper.client.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EsperContext {
    private static EPServiceProvider esperProvider;

    public static void initializeWith(EsperConfig... esperConfigs) {
        Configuration configuration = new Configuration();
        List<String> epls = new ArrayList<>();

        for (EsperConfig config : esperConfigs) {
            Map<String, Object> props = new HashMap<>();
            for (String property : config.getProperties()) {
                props.put(property, Object.class);
            }
            configuration.addEventType(config.getEventTypeName(), props);
            epls.add(config.getEpl());
        }
        final EPServiceProvider esperProvider = EPServiceProviderManager.getProvider(null, configuration);
        esperProvider.initialize();
        EPAdministrator esperAdmin = esperProvider.getEPAdministrator();
        for (String epl : epls) {
            esperAdmin.createEPL(epl, epl);
        }
        EsperContext.esperProvider = esperProvider;
    }


    public static void shutdown() {
        if (!esperProvider.isDestroyed()) {
            esperProvider.destroy();
        }
    }

    public static void sendEvent(Map<String, Object> data, String eventTypeName) {
        esperProvider.getEPRuntime().sendEvent(data, eventTypeName);
    }

    public static void addListener(UpdateListener updateListener, String epl) {
        final EPStatement epStatement = esperProvider.getEPAdministrator().getStatement(epl);
        epStatement.addListener(updateListener);
    }
}


