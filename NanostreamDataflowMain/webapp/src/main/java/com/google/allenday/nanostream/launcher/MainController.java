package com.google.allenday.nanostream.launcher;

import com.google.allenday.nanostream.launcher.worker.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
public class MainController {

    private final ListFetcher listFetcher;
    private final InfoFetcher infoFetcher;
    private final Starter starter;
    private final SettingsFetcher settingsFetcher;
    private final MetricsFetcher metricsFetcher;

    @Autowired
    public MainController(ListFetcher listFetcher, InfoFetcher infoFetcher, Starter starter,
                          SettingsFetcher settingsFetcher, MetricsFetcher metricsFetcher
    ) {
        this.listFetcher = listFetcher;
        this.infoFetcher = infoFetcher;
        this.starter = starter;
        this.settingsFetcher = settingsFetcher;
        this.metricsFetcher = metricsFetcher;
    }

    @CrossOrigin
    @PostMapping(value = "/launch", produces = APPLICATION_JSON_VALUE)
    public String launch(HttpServletRequest request) throws IOException {
        return starter.invoke(new LaunchParams(request));
    }

    @CrossOrigin
    @PostMapping(value = "/stop", produces = APPLICATION_JSON_VALUE)
    public String stop(HttpServletRequest request) throws IOException {
        return new Stopper(request).invoke();
    }

    @CrossOrigin
    @GetMapping(value = "/jobs", produces = APPLICATION_JSON_VALUE)
    public String jobs() throws IOException {
        return listFetcher.invoke();
    }

    @CrossOrigin
    @GetMapping(value = "/info", produces = APPLICATION_JSON_VALUE)
    public String info(@RequestParam String jobId, @RequestParam String location) throws IOException {
        return infoFetcher.invoke(jobId, location);
    }

    @CrossOrigin
    @GetMapping(value = "/settings", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<SettingsResponse> settings() throws IOException { // TODO: remove not used
        SettingsResponse response = settingsFetcher.invoke();
        return ResponseEntity.ok(response);
    }

    @CrossOrigin
    @GetMapping(value = "/should_stop", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<ShouldStopResponse> shouldStop(@RequestParam String jobId, @RequestParam String location) throws IOException {
        ShouldStopResponse response = metricsFetcher.shouldStop(jobId, location);
        return ResponseEntity.ok(response);
    }

}
