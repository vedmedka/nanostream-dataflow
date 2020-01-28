package com.google.allenday.nanostream.launcher.worker;

import com.google.api.MonitoredResource;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.TimeSeries;
import com.google.protobuf.Timestamp;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public abstract class TimeSeriesSummary<T> {
    private String name;
    private Timestamp mostRecentRunTime;
    T mostRecentValue;
    List<T> values;
    MonitoredResource monitoredResource;

    public static TimeSeriesSummary fromTimeSeries(TimeSeries timeSeries) {
        switch (timeSeries.getValueType()) {
            case STRING:
                return new StringTimeSeriesSummary(timeSeries);
            case INT64:
                return new Int64TimeSeriesSummary(timeSeries);
            default:
                return null;
        }
    }

    private TimeSeriesSummary(TimeSeries timeSeries) {
        name = timeSeries.getMetric().getType();
        monitoredResource = timeSeries.getResource();
    }

    Point getMostRecentPoint(TimeSeries timeSeries) {
        Point max = Collections.max(timeSeries.getPointsList(),
                Comparator.comparingLong(p -> p.getInterval().getEndTime().getSeconds()));
        mostRecentRunTime = max.getInterval().getEndTime();
        return max;
    }

    public String getName() {
        return name;
    }

    public T getMostRecentValue() {
        return mostRecentValue;
    }

    public Timestamp getMostRecentRunTime() {
        return mostRecentRunTime;
    }

    public List<T> getValues() {
        return values;
    }

    public MonitoredResource getMonitoredResource() {
        return monitoredResource;
    }

    public abstract T getAverage();



    public static class StringTimeSeriesSummary extends TimeSeriesSummary<String> {
        private StringTimeSeriesSummary(TimeSeries timeSeries) {
            super(timeSeries);
            Point max = getMostRecentPoint(timeSeries);
            if (max == null) {
                return;
            }
            mostRecentValue = max
                    .getValue()
                    .getStringValue();
            values = Lists.newArrayList(Collections2.transform(timeSeries.getPointsList(),
                    point -> point.getValue().getStringValue()));
        }

        @Override
        public String getAverage() {
            return values.stream().collect(Collectors.joining(","));
        }
    }

    public static class Int64TimeSeriesSummary extends TimeSeriesSummary<Long> {
        private Int64TimeSeriesSummary(TimeSeries timeSeries) {
            super(timeSeries);
            Point max = getMostRecentPoint(timeSeries);
            if (max == null) {
                return;
            }
            mostRecentValue = max
                    .getValue()
                    .getInt64Value();
            values = Lists.newArrayList(Collections2.transform(timeSeries.getPointsList(),
                    point -> point.getValue().getInt64Value()));
        }

        @Override
        public Long getAverage() {
            return values.stream().collect(Collectors.averagingLong(Long::longValue)).longValue();
        }
    }
}
