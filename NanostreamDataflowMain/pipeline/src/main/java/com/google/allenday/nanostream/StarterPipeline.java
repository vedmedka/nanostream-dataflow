package com.google.allenday.nanostream;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;

import java.io.IOException;

/**
 * A starter example for writing Beam programs.
 *
 * <p>The example listen to a PubSub subscription to
 * <li>
 * <ul>serialize the payload into a Google BigQuery table</ul>
 * <ul>Resend a dummy message to the topic of the associated listened subscription</ul>
 * <ul>If the dummy message is the only message received, send a message to another topic that will trigger an action such as killing this pipeline for instance</ul>
 * </li>
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 * --runner=DataflowRunner
 */
public class StarterPipeline {

    public interface StarterPipelineOptions extends PubsubOptions {

        @Description("Duration in seconds of a pane in a Global window")
        @Default.Long(5)
        Long getWindowDuration();

        void setWindowDuration(Long value);

        //TODO: Add new options: {project-id, input_subscription_name, dataset_name, table_name, somethin else? }
    }

    public static class FormatAsPubSubMessage extends SimpleFunction<Long, PubsubMessage> {
        @Override
        public PubsubMessage apply(Long message) {
            return new PubsubMessage(String.valueOf(message).getBytes(), null);
        }
    }

    public static class CreateDummyPubSubMessage extends SimpleFunction<Long, PubsubMessage> {
        @Override
        public PubsubMessage apply(Long message) {
            return new PubsubMessage("dummy".getBytes(), null);
        }
    }

    public static class CreateBQRow extends SimpleFunction<String, PubsubMessage> {
        @Override
        public PubsubMessage apply(String message) {
            return new PubsubMessage(message.getBytes(), null);
        }
    }

    static void runStarterPipeline(StarterPipelineOptions options) throws IOException {
        Pipeline p = Pipeline.create(options);

        PCollection<PubsubMessage> pubsubMessagePCollection = p.apply("Read PubSub messages", PubsubIO.readMessages().fromSubscription("projects/project-id/subscriptions/bar"))
                .apply("windowing pipeline with sessions window", Window.<PubsubMessage>into(new GlobalWindows())
                        .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(options.getWindowDuration()))))
                        //.triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1))) //TODO: combine number of element and elapsed time since the first pane element;
                        .discardingFiredPanes());

        PCollection<Long> numberOfMessage = pubsubMessagePCollection
                .apply("counting pub/sub message", Count.globally());

        PDone killingBranch = numberOfMessage
                .apply("Filtering when PCollection size equals to one", Filter.equal(1L))
                .apply("PubSub message creation", MapElements.via(new FormatAsPubSubMessage()))
                .apply("Publishing on PusbSub kill topic", PubsubIO.writeMessages().to("projects/project-id/topics/export"));

        PDone hearthBeatBranch = numberOfMessage
                .apply("Creating Dummy Message", MapElements.via(new CreateDummyPubSubMessage()))
                .apply("Publishing dummy message to the initial topic", PubsubIO.writeMessages().to("projects/project-id/topics/foo"));

        PDone serialisationBranch = pubsubMessagePCollection
                .apply("PubSub message payload extraction", ParDo.of(new DoFn<PubsubMessage, String>() {
                    @ProcessElement
                    public void processElement(@Element PubsubMessage pubsubMessage, OutputReceiver<String> receiver) {
                        String element = new String(pubsubMessage.getPayload());
                        receiver.output(element);
                    }
                }))
                .apply("Filtering out dummy messages", Filter.by(input -> !input.matches("dummy.*")))
                .apply("Creating bigQuery row with the message content", MapElements.via(new CreateBQRow()))
                .apply("Inserting data to BQ table", PubsubIO.writeMessages().to("projects/project-id/topics/bq_topic"));

        PipelineResult result = p.run();
        try {
            result.waitUntilFinish();
        } catch (Exception exc) {
            result.cancel();
        }
    }

    public static void main(String[] args) throws IOException {

        // from here we get the information from the command line argument
        // By default we return PipelineOptions entity. But we can return our own entity derived from PipelineOtions interfaces --> 'as(...)' method
        // In particular we define the pipeline runner through the options. If none is set, we use the DirectRunner (used to run pipeline locally)
        StarterPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(StarterPipelineOptions.class);

        options.setStreaming(true);
        options.setPubsubRootUrl("http://localhost:8085");

        runStarterPipeline(options);
    }
}
