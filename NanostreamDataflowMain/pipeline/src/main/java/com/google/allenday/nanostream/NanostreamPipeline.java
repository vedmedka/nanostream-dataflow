package com.google.allenday.nanostream;

import com.google.allenday.genomics.core.model.FileWrapper;
import com.google.allenday.genomics.core.model.SampleMetaData;
import com.google.allenday.genomics.core.pipeline.PipelineSetupUtils;
import com.google.allenday.genomics.core.processing.align.AlignTransform;
import com.google.allenday.nanostream.aligner.GetSequencesFromSamDataFn;
import com.google.allenday.nanostream.errorcorrection.ErrorCorrectionFn;
import com.google.allenday.nanostream.gcs.ParseGCloudNotification;
import com.google.allenday.nanostream.geneinfo.GeneData;
import com.google.allenday.nanostream.geneinfo.GeneInfo;
import com.google.allenday.nanostream.geneinfo.LoadGeneInfoTransform;
import com.google.allenday.nanostream.injection.MainModule;
import com.google.allenday.nanostream.kalign.ProceedKAlignmentFn;
import com.google.allenday.nanostream.kalign.SequenceOnlyDNACoder;
import com.google.allenday.nanostream.output.PrepareSequencesStatisticToOutputDbFn;
import com.google.allenday.nanostream.output.WriteDataToFirestoreDbFn;
import com.google.allenday.nanostream.probecalculation.KVCalculationAccumulatorFn;
import com.google.allenday.nanostream.pubsub.DecodeNotificationJsonMessage;
import com.google.allenday.nanostream.pubsub.FilterObjectFinalizeMessage;
import com.google.allenday.nanostream.pubsub.GCSSourceData;
import com.google.allenday.nanostream.taxonomy.GetResistanceGenesTaxonomyDataFn;
import com.google.allenday.nanostream.taxonomy.GetTaxonomyFromTree;
import com.google.allenday.nanostream.util.CoderUtils;
import com.google.allenday.nanostream.util.trasform.FlattenMapToKV;
import com.google.allenday.nanostream.util.trasform.RemoveValueDoFn;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static com.google.allenday.nanostream.ProcessingMode.RESISTANT_GENES;
import static java.lang.String.format;

public class NanostreamPipeline implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(NanostreamPipeline.class);

    private NanostreamPipelineOptions options;
    private Injector injector;
    private ProcessingMode processingMode;

    public NanostreamPipeline(NanostreamPipelineOptions options) {
        this.options = options;
        this.injector = Guice.createInjector(new MainModule.Builder().fromOptions(options).build());
        processingMode = ProcessingMode.findByLabel(options.getProcessingMode());
    }

    public void run() {
        PipelineSetupUtils.prepareForInlineAlignment(options);
        Pipeline pipeline = Pipeline.create(options);
        CoderUtils.setupCoders(pipeline, new SequenceOnlyDNACoder());

        //        PCollection<PubsubMessage> windowingPipelineWithSessionsWindow = pubsubMessagePCollection.apply("windowing pipeline with sessions window", Window.<PubsubMessage>into(
////                FixedWindows.of(Duration.standardSeconds(120)))
////                        .withAllowedLateness(Duration.ZERO)
//////                        .triggering(AfterPane.elementCountAtLeast(0))
////                        .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(120))))
////                        .discardingFiredPanes());
//                new GlobalWindows())
//                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(120))))
//                //.triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1))) //TODO: combine number of element and ellapsed time since the first pane element;
//                .withAllowedLateness(Duration.ZERO)
//                .discardingFiredPanes());


//        SumMsgs sumMsgs = new SumMsgs();
//        PCollection<Long> numberOfMessage = windowingPipelineWithSessionsWindow
//                .apply("counting pub/sub message", Combine.globally(sumMsgs).withoutDefaults());

//        windowingPipelineWithSessionsWindow
////                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(30))))
//                .apply("Count messages", Combine.globally(new SumMsgs1()).withoutDefaults());

//        PCollection<Long> numberOfMessage = windowingPipelineWithSessionsWindow
//                .apply("counting pub/sub message", Count.globally());
////                .apply("Count messages", Combine.globally(new SumMsgsLong()).withoutDefaults());

//        PDone hearthBeatBranch = numberOfMessage
//                .apply("Creating Dummy Message", MapElements.via(new StarterPipeline.CreateDummyPubSubMessage()))
//                .apply("Publishing dummy message to the initial topic", PubsubIO.writeMessages().to("projects/tas-nanostream-test1/topics/tas-nanostream-test1-pubsub-topic"));

//        PDone killingBranch = numberOfMessage
//                .apply("Filtering when PCollection size equals to one", Filter.equal(1L))
//                .apply("PubSub message creation", MapElements.via(new StarterPipeline.FormatAsPubSubMessage()))
//                .apply("Publishing on PusbSub kill topic", PubsubIO.writeMessages().to("projects/tas-nanostream-test1/topics/test-remove"));


//        windowingPipelineWithSessionsWindow
//                .apply(ParDo.of(new LoopingStatefulTimer(Instant.parse("2000-01-01T00:04:00Z"))));

        PCollection<KV<SampleMetaData, List<FileWrapper>>> filteredMessages = pipeline
                .apply("Reading PubSub", PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInputDataSubscription()))
                .apply("Filter only ADD FILE", ParDo.of(new FilterObjectFinalizeMessage()))
                .apply("Deserialize messages", ParDo.of(new DecodeNotificationJsonMessage()))
                .apply("Parse GCloud notification", ParDo.of(injector.getInstance(ParseGCloudNotification.class)));

//        filteredMessages
//                .apply(options.getAlignmentWindow() + "s FastQ collect window",
//        Window.into(FixedWindows.of(Duration.standardSeconds(options.getAlignmentWindow())))
//                        Window.<KV<SampleMetaData, List<FileWrapper>>>into(FixedWindows.of(Duration.standardSeconds(options.getAlignmentWindow())))
//                                .triggering(AfterPane.elementCountAtLeast(1))
//                                .withAllowedLateness(Duration.ZERO)
//                                .discardingFiredPanes()
//                )
//                .apply("Alignment", injector.getInstance(AlignTransform.class))
//                .apply("Extract Sequences",
//                        ParDo.of(injector.getInstance(GetSequencesFromSamDataFn.class)))
//                .apply("Group by SAM reference", GroupByKey.create())
//                .apply("K-Align", ParDo.of(injector.getInstance(ProceedKAlignmentFn.class)))
//                .apply("Error correction", ParDo.of(new ErrorCorrectionFn()))
//                .apply("Remove Sequence part", ParDo.of(new RemoveValueDoFn<>()))
//                .apply("Get Taxonomy data", getTaxonomyData(pipeline))
//                .apply("Global Window with Repeatedly triggering" + options.getStatisticUpdatingDelay(),
//                        Window.<KV<KV<GCSSourceData, String>, GeneData>>into(new GlobalWindows())
//                                .triggering(Repeatedly.forever(AfterProcessingTime
//                                        .pastFirstElementInPane()
//                                        .plusDelayOf(Duration.standardSeconds(options.getStatisticUpdatingDelay()))))
//                                .withAllowedLateness(Duration.ZERO)
//                                .accumulatingFiredPanes())
//                .apply("Accumulate results to Map", Combine.globally(new KVCalculationAccumulatorFn()))
//                .apply("Flatten result map", ParDo.of(new FlattenMapToKV<>()))
//                .apply("Prepare sequences statistic to output",
//                        ParDo.of(injector.getInstance(PrepareSequencesStatisticToOutputDbFn.class)))
//                .apply("Write sequences statistic to Firestore",
//                        ParDo.of(injector.getInstance(WriteDataToFirestoreDbFn.class)))
//        ;


        filteredMessages
                .apply(Window.into(new GlobalWindows()))
                .apply(ParDo.of(new LoopingStatefulTimer()));
        
//                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(30))))
//                .apply("Count messages", Combine.globally(new SumMsgs()).withoutDefaults());

        PipelineResult pipelineResult = pipeline.run();
//        MetricQueryResults metricQueryResults = pipelineResult.metrics().allMetrics();
//        logger.info("metricQueryResults: " + metricQueryResults.toString());
//        pipelineResult.cancel();


//        MetricResults metrics = pipelineResult.metrics();
//        MetricQueryResults metricResults = metrics.queryMetrics(MetricsFilter.builder()
//                .addNameFilter(MetricNameFilter.named("my-counter", "my-counter"))
//                .addStep("Count messages")
//                .build());
//        Iterable<MetricResult<Long>> counters = metricResults.getCounters();
    }

    private ParDo.SingleOutput<KV<GCSSourceData, String>, KV<KV<GCSSourceData, String>, GeneData>> getTaxonomyData(Pipeline pipeline) {
        ParDo.SingleOutput<KV<GCSSourceData, String>, KV<KV<GCSSourceData, String>, GeneData>> taxonomy;
        if (processingMode == RESISTANT_GENES) {
            PCollection<KV<String, GeneInfo>> geneInfoMapPCollection = pipeline.apply(injector.getInstance(LoadGeneInfoTransform.class));
            PCollectionView<Map<String, GeneInfo>> geneInfoMapPCollectionView = geneInfoMapPCollection.apply(View.asMap());
            taxonomy = ParDo.of(injector.getInstance(GetResistanceGenesTaxonomyDataFn.class)
                    .setGeneInfoMapPCollectionView(geneInfoMapPCollectionView))
                    .withSideInputs(geneInfoMapPCollectionView);
        } else {
            taxonomy = ParDo.of(injector.getInstance(GetTaxonomyFromTree.class));
        }
        return taxonomy;
    }

    public static class SumMsgsLong implements SerializableFunction<Iterable<Long>, Long> {

//        private int sum;

        @Override
        public Long apply(Iterable<Long> input) {
            long res = 0;

            int count = 0;
            for (Long p : input) {
                logger.info(format("Input messages count(%d), %d", count++, p));
                res = p;
            }

            return res ;
        }
    }

    public static class SumMsgs1 implements SerializableFunction<Iterable<PubsubMessage>, PubsubMessage> {

//        private int sum;

        @Override
        public PubsubMessage apply(Iterable<PubsubMessage> input) {
            int sum = 0;

            PubsubMessage pubsubMessage = null;
            for (PubsubMessage p : input) {
                pubsubMessage = p;
                sum++;
            }
            logger.info("Num messages: " + sum);
            return pubsubMessage ;
        }
    }


    public static class AverageFn extends Combine.CombineFn<Integer, AverageFn.Accum, Double> {
        public static class Accum {
            int sum = 0;
            int count = 0;
        }

        @Override
        public Accum createAccumulator() { return new Accum(); }

        @Override
        public Accum addInput(Accum accum, Integer input) {
            accum.sum += input;
            accum.count++;
            return accum;
        }

        @Override
        public Accum mergeAccumulators(Iterable<Accum> accums) {
            Accum merged = createAccumulator();
            for (Accum accum : accums) {
                merged.sum += accum.sum;
                merged.count += accum.count;
            }
            return merged;
        }

        @Override
        public Double extractOutput(Accum accum) {
            return ((double) accum.sum) / accum.count;
        }
    }


    public static class LoopingStatefulTimer extends DoFn<KV<SampleMetaData, List<FileWrapper>>, KV<SampleMetaData, List<FileWrapper>>> {

        Instant stopTimerTime;

        LoopingStatefulTimer(){
        }

        @StateId("loopingTimerTime")
        private final StateSpec<ValueState<Long>> loopingTimerTime =
                StateSpecs.value(BigEndianLongCoder.of());

        @StateId("key")
        private final StateSpec<ValueState<KV<SampleMetaData, List<FileWrapper>>>> key =
                StateSpecs.value();

        @TimerId("loopingTimer")
        private final TimerSpec loopingTimer =
                TimerSpecs.timer(TimeDomain.EVENT_TIME);

        @ProcessElement public void process(ProcessContext c, @StateId("key") ValueState<KV<SampleMetaData, List<FileWrapper>>> key,
                                            @StateId("loopingTimerTime") ValueState<Long> loopingTimerTime,
                                            @TimerId("loopingTimer") Timer loopingTimer) {

            // If the timer has been set already, or if the value is smaller than
            // the current element + window duration, do not set
            Long currentTimerValue = loopingTimerTime.read();
            Instant nextTimerTimeBasedOnCurrentElement = c.timestamp().plus(Duration.standardMinutes(1));

            if (currentTimerValue == null || currentTimerValue >
                    nextTimerTimeBasedOnCurrentElement.getMillis()) {
                loopingTimer.set(nextTimerTimeBasedOnCurrentElement);
                loopingTimerTime.write(nextTimerTimeBasedOnCurrentElement.getMillis());
                logger.info("Process element: Timer set to " + nextTimerTimeBasedOnCurrentElement);
            }

            // We need this value so that we can output a value for the correct key in OnTimer
            if (key.read() == null) {
                key.write(c.element());
            }

            c.output(c.element());
        }

        @OnTimer("loopingTimer")
        public void onTimer(
                OnTimerContext c,
                @StateId("key") ValueState<KV<SampleMetaData, List<FileWrapper>>> key,
                @TimerId("loopingTimer") Timer loopingTimer) {

            logger.info("Timer @ {} fired", c.timestamp());
            KV<SampleMetaData, List<FileWrapper>> read = key.read();
            c.output(read);

            // If we do not put in a “time to live” value, then the timer would loop forever
            Instant nextTimer = c.timestamp().plus(Duration.standardMinutes(1));
            loopingTimer.set(nextTimer);
            logger.info("OnTimer: Timer set to " + nextTimer);

//            if (nextTimer.isBefore(stopTimerTime)) {
//                loopingTimer.set(nextTimer);
//            } else {
//                logger.info(
//                        "Timer not being set as exceeded Stop Timer value {} ",
//                        stopTimerTime);
//            }
        }
    }
}
