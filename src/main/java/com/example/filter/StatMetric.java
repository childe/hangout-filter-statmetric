package com.example.filter;

import java.io.IOException;
import java.util.*;

import com.ctrip.ops.sysdev.baseplugin.BaseFilter;

import com.ctrip.ops.sysdev.render.TemplateRender;
import org.apache.log4j.Logger;


public class StatMetric extends BaseFilter {
    private static final Logger logger = Logger.getLogger(StatMetric.class.getName());

    int windowSize;
    String keyField;
    TemplateRender key;
    TemplateRender value;
    Map<String, Object> metric;
    Map<String, Object> metricToEmit;
    long lastEmitTime;

    public StatMetric(Map config) {
        super(config);
    }

    protected void prepare() {
        this.keyField = (String) config.get("key");
        try {
            this.key = TemplateRender.getRender(this.keyField, false);
        } catch (IOException e) {
            logger.fatal("could not build render from " + this.keyField);
            System.exit(1);
        }
        String valueField = (String) config.get("value");
        try {
            this.value = TemplateRender.getRender(valueField, false);
        } catch (IOException e) {
            logger.fatal("could not build render from " + valueField);
            System.exit(1);
        }
        if (!config.containsKey("windowSize")) {
            logger.fatal("windowSize must be included in config");
            System.exit(1);
        }
        this.windowSize = (int) config.get("windowSize") * 1000;

        this.processExtraEventsFunc = true;
        this.metric = new HashMap();
        this.metricToEmit = new HashMap();

        this.lastEmitTime = System.currentTimeMillis();
    }

    @Override
    protected Map filter(final Map event) {
        if (System.currentTimeMillis() >= this.windowSize + this.lastEmitTime) {
            this.metricToEmit = this.metric;
            this.metric = new HashMap();
        }
        Object keyValueObj = this.key.render(event);
        if (keyValueObj == null) {
            return event;
        }

        String keyValue = keyValueObj.toString();

        Object valueValueObj = this.value.render(event);
        if (valueValueObj == null) {
            return event;
        }
        float valueValue;
        try {
            valueValue = Float.valueOf(valueValueObj.toString().trim());
        } catch (Exception e) {
            return event;
        }

        HashMap<String, Float> stat = (HashMap) this.metric.get(keyValue);
        if (stat == null) {
            stat = new HashMap() {{
                this.put("count", 1.0f);
                this.put("min", valueValue);
                this.put("mean", valueValue);
                this.put("max", valueValue);
                this.put("sum", valueValue);
            }};
        } else {
            stat.put("count", stat.get("count") + 1.0f);
            stat.put("min", Math.min(stat.get("min"), valueValue));
            stat.put("max", Math.max(stat.get("max"), valueValue));
            stat.put("sum", stat.get("sum") + valueValue);
            stat.put("mean", stat.get("sum") / stat.get("count"));
        }
        this.metric.put(keyValue, stat);

        return event;
    }

    @Override
    public List<Map<String, Object>> filterExtraEvents(Map event) {
        if (metricToEmit.size() == 0) {
            return null;
        }
        List<Map<String, Object>> events = new ArrayList();
        this.metricToEmit.forEach((url, stat) -> {
            HashMap emitEvent = new HashMap() {{
                this.put(keyField, url);
                this.put("stat", stat);
                this.put("@timestamp", lastEmitTime);
            }};
            this.postProcess(emitEvent, true);
            events.add(emitEvent);
        });

        this.metricToEmit.clear();
        this.lastEmitTime = System.currentTimeMillis();

        return events;
    }
}
