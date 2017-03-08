在查看访问日志的时候,有个常见的需求是"找出响应时间最高的URL". 如果量很大的话,这个请求会消耗比较长的时间甚至超时. 有没有想过在hangout里面就做好这个聚合, 然后存到ES中.

我们可以写一个filter plugin来实现这个效果. 初步想实现的功能如下, 将每分钟(可配置)内的数据做聚合,得到每个Url的响应时间的统计值, 包括count, mean, min, max, sum. 每分钟得到一个新的metric事件, 将其输出到下游(可能是新的filter, 也可能是output比如ES)

也可以像前两个例子一样, 继承BaseFilter, 然后实现自己的filter方法. 在filter里面, 就是实现聚合计算url和响应时间,大概如下:

```
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
```

显然这个 `this.metric` 是一个独立的event, 它不能附加到每一条日志事件中去. 所以还需要重写另外一个函数, 来处理这种特殊的情况.

这个函数是用来输入额外的日志事件, 可以输出多条, 所以返回了一个List, 比如有多个Url, 可以每个输出一条. 我这里写的代码如下, 只输出了一条, 对this.metric中的内容做了打平处理.

**注意 在prepare里, 需要设置一个成员变量 `this.processExtraEventsFunc = true`;**

```
@Override
public List<Map<String, Object>> filterExtraEvents(Map event) {
    if (metricToEmit.size() == 0) {
        return null;
    }

    ArrayList FlatMetrics = new ArrayList<>();
    this.metricToEmit.forEach((url, stat) -> {
        FlatMetrics.add(new HashMap() {{
            this.put(keyField, url);
            this.put("stat", stat);
        }});

    });
    HashMap emitEvent = new HashMap() {{
        this.put("@timestamp", lastEmitTime);
        this.put("metrics", FlatMetrics);
    }};
    this.postProcess(emitEvent, true);

    this.metricToEmit.clear();
    this.lastEmitTime = System.currentTimeMillis();

    List<Map<String, Object>> events = new ArrayList() {{
        this.add(emitEvent);
    }};
    return events;
}
```

来看下效果吧:

    / 0.1
    {@timestamp=2017-03-08T12:12:37.424+08:00, time=0.1, message=/ 0.1, url=/}
    / 0.2
    {@timestamp=2017-03-08T12:12:39.062+08:00, time=0.2, message=/ 0.2, url=/}
    /hello 0.31
    {@timestamp=2017-03-08T12:12:44.110+08:00, time=0.31, message=/hello 0.31, url=/hello}
    /hello 0.14
    {@timestamp=2017-03-08T12:12:48.573+08:00, time=0.14, message=/hello 0.14, url=/hello}
    / 3
    {@timestamp=2017-03-08T12:12:50.278+08:00, time=3, message=/ 3, url=/}
    / 1
    {@timestamp=2017-03-08T12:12:51.541+08:00, time=1, message=/ 1, url=/}
    /hello 0.13
    {@timestamp=2017-03-08T12:12:54.909+08:00, time=0.13, message=/hello 0.13, url=/hello}
    / 1
    {@timestamp=2017-03-08T12:14:16.857+08:00, time=1, message=/ 1, url=/}
    {@timestamp=1488946354979, metrics=[{stat={min=0.1, max=3.0, mean=1.075, count=4.0, sum=4.3}, url=/}, {stat={min=0.13, max=0.31, mean=0.19333333, count=3.0, sum=0.58}, url=/hello}], statmetric=true}

**注意**

因为之前的hangout的整个结构没有考虑到这种情况, 也是后来在向前兼容的基本上又改的. 所以第一在实现上面不太好看, 第二有个问题:如果处理了59秒的数据之后, 再也没有新的处理来了, 那这59秒的汇总结果永远不会输入. 而且是10分钟之后才被激活,那么它延时了10分钟.  
后面会思考如何解决这个问题.

最后记得补充单元测试~
