package com.jayway.perfectstorm.esper;

import com.espertech.esper.client.*;

import java.util.HashMap;
import java.util.Map;

public class EsperContext {
    private final EPServiceProvider esperProvider;
    private final String eventTypeName;

    private EsperContext(EPServiceProvider esperProvider, String eventTypeName) {
        this.esperProvider = esperProvider;
        this.eventTypeName = eventTypeName;
    }

    public static EsperContext create(UpdateListener listener, String eplStatement, String eventTypeName, String... properties) {
        Configuration configuration = new Configuration();

        Map<String, Object> props = new HashMap<>();
        for (String property : properties) {
            props.put(property, Object.class);
        }
        configuration.addEventType(eventTypeName, props);

        final EPServiceProvider esperProvider = EPServiceProviderManager.getProvider(null, configuration);

        esperProvider.initialize();
        EPAdministrator esperAdmin = esperProvider.getEPAdministrator();
        EPStatement epStatement = esperAdmin.createEPL(eplStatement);
        epStatement.addListener(listener);
        return new EsperContext(esperProvider, eventTypeName);
    }


    public void shutdown() {
        if (!esperProvider.isDestroyed()) {
            esperProvider.destroy();
        }
    }

    public void sendEvent(Map<String, Object> data) {
        esperProvider.getEPRuntime().sendEvent(data, eventTypeName);
    }
}


