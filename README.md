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

这个函数是用来输入额外的日志事件, 可以输出多条, 所以返回是一个List, 比如有多个Url, 可以每个输出一条.

**注意 在prepare里, 需要设置一个成员变量 `this.processExtraEventsFunc = true`;**

```
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
```

来看下效果吧:

    2017-03-08 16:08:40,528 INFO com.ctrip.ops.sysdev.core.Main main build input Stdin done
    / 1
    {@timestamp=2017-03-08T16:08:41.569+08:00, time=1, message=/ 1, url=/}
    / 2
    {@timestamp=2017-03-08T16:08:43.102+08:00, time=2, message=/ 2, url=/}
    hello 1
    {@timestamp=2017-03-08T16:08:45.262+08:00, time=1, message=hello 1, url=hello}
    / 3.5
    {@timestamp=2017-03-08T16:08:48.854+08:00, time=3.5, message=/ 3.5, url=/}
    hello 2
    {@timestamp=2017-03-08T16:08:51.958+08:00, time=2, message=hello 2, url=hello}
    gogogo 1
    {@timestamp=2017-03-08T16:10:02.001+08:00, time=1, message=gogogo 1, url=gogogo}
    {stat={min=1.0, max=2.0, mean=1.5, count=2.0, sum=3.0}, @timestamp=1488960520522, url=hello, statmetric=true}
    {stat={min=1.0, max=3.5, mean=2.1666667, count=3.0, sum=6.5}, @timestamp=1488960520522, url=/, statmetric=true}

**注意**

因为之前的hangout的整个结构没有考虑到这种情况, 也是后来在向前兼容的基本上又改的. 所以第一在实现上面太不好看, 第二有个问题:如果处理了59秒的数据之后, 再也没有新的处理来了, 那这59秒的汇总结果永远不会输入. 而且是10分钟之后才被激活,那么它延时了10分钟.  
后面会思考如何解决这个问题.

最后记得补充单元测试~
