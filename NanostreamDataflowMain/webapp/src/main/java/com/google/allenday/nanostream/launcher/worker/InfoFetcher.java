package com.google.allenday.nanostream.launcher.worker;

import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import static com.google.allenday.nanostream.launcher.worker.PipelineUtil.*;
import static java.lang.String.format;

@Service
public class InfoFetcher {

    private final String project;

    public InfoFetcher() {
        project = getProjectId();
    }

    public String invoke(String jobId, String location) throws IOException {
        HttpURLConnection connection = sendGetJobsInfoRequest(jobId, location);

        return getRequestOutput(connection);
    }

    private HttpURLConnection sendGetJobsInfoRequest(String jobId, String location) throws IOException {
        return sendRequest("GET", getUrl(jobId, location), null);
    }

    private URL getUrl(String jobId, String location) throws MalformedURLException {
        // see https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.jobs/get
        return new URL(format(DATAFLOW_API_BASE_URI + "projects/%s/jobs/%s?view=%s&location=%s", project, jobId, "JOB_VIEW_ALL", location));
    }

}
