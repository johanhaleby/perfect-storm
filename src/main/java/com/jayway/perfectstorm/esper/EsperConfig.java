package com.jayway.perfectstorm.esper;

public class EsperConfig {

    private final String epl;
    private final String eventTypeName;
    private final String[] properties;

    public EsperConfig(String epl, String eventTypeName, String... properties) {
        this.epl = epl;
        this.eventTypeName = eventTypeName;
        this.properties = properties;
    }

    public String getEpl() {
        return epl;
    }

    public String getEventTypeName() {
        return eventTypeName;
    }

    public String[] getProperties() {
        return properties;
    }
}
