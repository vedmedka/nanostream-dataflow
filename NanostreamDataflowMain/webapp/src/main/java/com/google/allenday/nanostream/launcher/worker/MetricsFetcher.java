package com.google.allenday.nanostream.launcher.worker;

import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.cloud.monitoring.v3.MetricServiceClient.ListTimeSeriesPagedResponse;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.monitoring.v3.ListTimeSeriesRequest;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.allenday.nanostream.launcher.worker.PipelineUtil.*;
import static com.google.common.collect.Lists.newArrayList;
import static com.jayway.jsonpath.JsonPath.parse;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

@Service
public class MetricsFetcher {
    private static final Logger logger = LoggerFactory.getLogger(MetricsFetcher.class);

    private final String project;
    private final InfoFetcher infoFetcher;
    private final MetricServiceClient client;

    public MetricsFetcher(InfoFetcher infoFetcher) throws IOException {
        this.infoFetcher = infoFetcher;
        this.project = getProjectId();
        this.client = MetricServiceClient.create();
    }

    public ShouldStopResponse shouldStop(String jobId, String location) throws IOException {
//        HttpURLConnection connection = sendGetMetricsRequest();
//
//        String requestOutput = getRequestOutput(connection);
//        return requestOutput;

        final String inputDataSubscription = getInputDataSubscription(jobId, location);

        ListTimeSeriesRequest listTimeSeriesRequest = getListTimeSeriesRequest();
        ListTimeSeriesPagedResponse listTimeSeriesResponse = client.listTimeSeries(listTimeSeriesRequest);
        List<TimeSeriesSummary> summaries = toTimeSeriesSummary(listTimeSeriesResponse);

//        "resource": {
//            "type": "pubsub_subscription",
//                    "labels": {
//                "subscription_id": "tas-nanostream-test1-upload-subscription",
//                        "project_id": "tas-nanostream-test1"
//            }
//        },
        Optional<TimeSeriesSummary> summaryOptional = summaries.stream().filter(summary -> {
            if (!"pubsub_subscription".equals(summary.getMonitoredResource().getType())) {
                return false;
            }
            Map<String, String> labelsMap = summary.getMonitoredResource().getLabelsMap();
            String projectId = labelsMap.get("project_id");
            String subscriptionId = labelsMap.get("subscription_id");
            return format("projects/%s/subscriptions/%s", projectId, subscriptionId).equals(inputDataSubscription);
        }).limit(1).findFirst();

        if (summaryOptional.isPresent()) {
            TimeSeriesSummary summary = summaryOptional.get();
            logger.info(summary.getMonitoredResource().getType());
            logger.info(Joiner.on("; ").withKeyValueSeparator("=").join(summary.getMonitoredResource().getLabelsMap()));
            Timestamp mostRecentRunTime = summary.getMostRecentRunTime();
            Long mostRecentValue = (Long)summary.getMostRecentValue();
            logger.info(String.valueOf(mostRecentRunTime));
            return new ShouldStopResponse(jobId, Timestamps.toString(mostRecentRunTime), mostRecentValue);
        }
        else {
            return new ShouldStopResponse(jobId);
        }
    }

    private String getInputDataSubscription(String jobId, String location) throws IOException {
        String jobInfoStr = infoFetcher.invoke(jobId, location);
//        JsonNode jsonNode = new ObjectMapper().readValue(jobInfoStr, JsonNode.class);
//        JsonNode environment = jsonNode.get("environment");
//        JsonNode sdkPipelineOptions = environment.get("sdkPipelineOptions");
//        JsonNode options = sdkPipelineOptions.get("options");
//        JsonNode inputDataSubscription = options.get("inputDataSubscription");
//        String s = inputDataSubscription.asText();
//        logger.info("inputDataSubscription: " + s);
//        projects/tas-nanostream-test1/subscriptions/tas-nanostream-test1-upload-subscription

        // TODO: add json sample here
        return parse(jobInfoStr).read("$.environment.sdkPipelineOptions.options.inputDataSubscription");
//        String inputDataSubscription = (String)parse(jobInfoStr).read("$.environment.sdkPipelineOptions.options.inputDataSubscription");
//        String[] items = inputDataSubscription.split("/");
//        return items[items.length - 1];
    }

    private List<TimeSeriesSummary> toTimeSeriesSummary(ListTimeSeriesPagedResponse listTimeSeriesResponse) {
        List<TimeSeries> timeSeries = newArrayList(listTimeSeriesResponse.iterateAll());

        List<TimeSeriesSummary> summaries = Lists.newArrayList();

        summaries.addAll(timeSeries
                .stream()
                .map(TimeSeriesSummary::fromTimeSeries)
                .collect(toList()));
        return summaries;
    }

    private ListTimeSeriesRequest getListTimeSeriesRequest() {
        Timestamp now = Timestamps.fromMillis(System.currentTimeMillis());
        Duration duration5Min = Duration.newBuilder()
                .setSeconds(60L * 5L)  // 5 minutes
                .build();
        Timestamp startTime = Timestamps.subtract(now, duration5Min);
        TimeInterval interval = TimeInterval.newBuilder()
                .setStartTime(startTime)
                .setEndTime(now)
                .build();

        return ListTimeSeriesRequest
                .newBuilder()
                .setName(format("projects/%s", project))
                // https://cloud.google.com/monitoring/api/metrics_gcp#gcp-pubsub
                // Number of unacknowledged messages (a.k.a. backlog messages) in a subscription.
                // Sampled every 60 seconds. After sampling, data is not visible for up to 120 seconds.
                // metric.type="pubsub.googleapis.com/subscription/num_undelivered_messages"
                .setFilter(format("metric.type = \"%s\"", "pubsub.googleapis.com/subscription/num_undelivered_messages"))
                .setInterval(interval)
                .build();
    }

//    private HttpURLConnection sendGetMetricsRequest() throws IOException {
//        return sendRequest("GET", getUrl(), null);
//    }
//
//    private URL getUrl() throws MalformedURLException {
//        // see https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.timeSeries/list
//
////        URLEncoder.encode
//        return new URL(format(MONITORING_API_BASE_URI + "projects/%s/timeSeries", project));
//    }



//    https://monitoring.googleapis.com/v3/{name}/timeSeries
//
//    https://content-monitoring.googleapis.com/v3/projects/tas-nanostream-test1/timeSeries
// ?filter=metric.type%3D%22pubsub.googleapis.com%2Fsubscription%2Fnum_undelivered_messages%22&interval.startTime=2020-01-01T15%3A05%3A23.045123456Z&interval.endTime=2020-01-01T15%3A10%3A23.045123456Z
}
