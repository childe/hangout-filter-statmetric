package com.example.filter.test;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.example.filter.StatMetric;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.Yaml;


public class TestStatMetric {
    private ArrayList<Map<String, Object>> process(StatMetric filter, ArrayList<Map<String, Object>> events) {
        for (int i = 0; i < events.size(); i++) {
            Map rst = filter.process(events.get(i));
            if (rst != null) {
                events.set(i, rst);
            }
        }
        int originEventSize = events.size();
        for (int i = 0; i < originEventSize; i++) {
            List rst = filter.processExtraEvents(events.get(i));
            if (rst != null) {
                events.addAll(rst);
            }
        }
        return events;
    }

    @Test
    public void testMetric() {
        String c = String
                .format("%s\n%s\n%s\n",
                        "key: url",
                        "value: response_time",
                        "windowSize: 1");
        Yaml yaml = new Yaml();
        Map config = (Map) yaml.load(c);
        Assert.assertNotNull(config);

        StatMetric filter = new StatMetric(config);

        Map<String, Object> event = new HashMap();
        event.put("url", "/");
        event.put("response_time", "0.1");

        ArrayList<Map<String, Object>> events = new ArrayList();
        events.add(event);
        process(filter, events);

        event = new HashMap();
        event.put("url", "/hello");
        event.put("response_time", "0.1");
        events = new ArrayList();
        events.add(event);
        process(filter, events);

        event = new HashMap();
        event.put("url", "/");
        event.put("response_time", "0.1");
        events = new ArrayList();
        events.add(event);
        process(filter, events);

        event = new HashMap();
        event.put("url", "/");
        event.put("response_time", "0.4");
        events = new ArrayList();
        events.add(event);
        process(filter, events);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        event = new HashMap();
        event.put("url", "/");
        event.put("response_time", "0.1");
        events = new ArrayList();
        events.add(event);
        process(filter, events);

        System.out.println(events.get(1));

        Assert.assertEquals(events.size(), 2);

        List<Map> metrics = (List) events.get(1).get("metrics");
        Assert.assertEquals(metrics.size(), 2);

        for (Map metric : metrics) {
            String url = (String) metric.get("url");
            if (url.equalsIgnoreCase("/")) {
                Map stat = (Map) metric.get("stat");
                Assert.assertEquals(stat.get("count"), 3.0f);
                Assert.assertEquals(stat.get("min"), 0.1f);
                Assert.assertEquals(stat.get("mean"), 0.2f);
                Assert.assertEquals(stat.get("max"), 0.4f);
                Assert.assertEquals(stat.get("sum"), 0.6f);
            } else {
                Map stat = (Map) metric.get("stat");
                Map metric2 = (Map) events.get(1).get("/hello");
                Assert.assertEquals(stat.get("count"), 1.0f);
                Assert.assertEquals(stat.get("min"), 0.1f);
                Assert.assertEquals(stat.get("mean"), 0.1f);
                Assert.assertEquals(stat.get("max"), 0.1f);
                Assert.assertEquals(stat.get("sum"), 0.1f);
            }
        }


    }
}
