package com.google.allenday.nanostream.launcher.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.*;

import static com.google.allenday.nanostream.launcher.worker.PipelineUtil.*;

import org.json.JSONObject;

import static com.google.appengine.api.ThreadManager.backgroundThreadFactory;
import static com.google.appengine.api.ThreadManager.createBackgroundThread;
import static java.lang.String.format;

@Service
public class Initializer {

    private static final Logger logger = LoggerFactory.getLogger(Initializer.class);

    private final String project;
    private final String FIREBASE_API = "https://firebase.googleapis.com/v1beta1/";


    public Initializer() {
        this.project = getProjectId();
    }

    @PostConstruct
    public void init() throws IOException {
        logger.info("PostConstruct called");
//        addProjectToFirebase();
//        getFirebaseProjects();
        getFirebaseProjectInfo();
//        runBackgroundThread();
    }

    private void addProjectToFirebase() throws IOException {
        HttpURLConnection connection = sendAddProjectToFirebaseRequest();
        String output = getRequestOutput(connection);
        logger.info(output);
    }

    private void getFirebaseProjects() throws IOException {
        HttpURLConnection connection = sendGetFirebaseProjectsRequest();
        String output = getRequestOutput(connection);
        logger.info(output);
    }

    private void getFirebaseProjectInfo() throws IOException {
        HttpURLConnection connection = sendGetFirebaseProjectInfoRequest();
        String output = getRequestOutput(connection);
        logger.info(output);
    }

    private HttpURLConnection sendAddProjectToFirebaseRequest() throws IOException {
        String url = format(FIREBASE_API + "projects/%s:addFirebase?alt=json", project);
        logger.info(format("Send request add project to firebase: %s", url));
        return sendRequest("POST", new URL(url), new JSONObject());
    }

    private HttpURLConnection sendGetFirebaseProjectsRequest() throws IOException {
        String url = FIREBASE_API + "projects";
        logger.info(format("Send request get firebase projects: %s", url));
        return sendRequest("GET", new URL(url), null);
    }

    private HttpURLConnection sendGetFirebaseProjectInfoRequest() throws IOException {
        String url = format(FIREBASE_API + "projects/%s", project);
        logger.info(format("Send request get firebase project info: %s", url));
        return sendRequest("GET", new URL(url), null);
    }

    private void runBackgroundThread() {
        ThreadFactory threadFactory = backgroundThreadFactory();
//        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1, threadFactory);
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        long period = 10;
        long initialDelay = 30;
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                logger.info("background thread here");
            }
        }, initialDelay, period, TimeUnit.SECONDS);

//        createBackgroundThread(new Runnable() {
//
//            @Override
//            public void run() {
//
//            }
//        });
    }
}
