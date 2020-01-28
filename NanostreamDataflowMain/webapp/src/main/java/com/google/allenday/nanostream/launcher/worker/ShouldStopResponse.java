package com.google.allenday.nanostream.launcher.worker;

public class ShouldStopResponse {
    private String jobId;
    private String timestamp;
    private String numMessages;

    public ShouldStopResponse(String jobId, String timestamp, Long numMessages) {
        this.jobId = jobId;
        this.timestamp = timestamp;
        this.numMessages = String.valueOf(numMessages);
    }

    public ShouldStopResponse(String jobId) {
        this.jobId = jobId;
    }

    public String getJobId() {
        return jobId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getNumMessages() {
        return numMessages;
    }

    public void setNumMessages(String numMessages) {
        this.numMessages = numMessages;
    }
}
