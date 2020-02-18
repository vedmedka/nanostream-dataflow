package com.google.allenday.nanostream;

import com.google.allenday.genomics.core.pipeline.PipelineSetupUtils;
import com.google.allenday.genomics.core.processing.align.AlignTransform;
import com.google.allenday.nanostream.aligner.GetSequencesFromSamDataFn;
import com.google.allenday.nanostream.batch.CreateBatchesTransform;
import com.google.allenday.nanostream.gcs.ParseGCloudNotification;
import com.google.allenday.nanostream.geneinfo.GeneData;
import com.google.allenday.nanostream.geneinfo.GeneInfo;
import com.google.allenday.nanostream.geneinfo.LoadGeneInfoTransform;
import com.google.allenday.nanostream.injection.MainModule;
import com.google.allenday.nanostream.output.PrepareSequencesStatisticToOutputDbFn;
import com.google.allenday.nanostream.output.WriteDataToFirestoreDbFn;
import com.google.allenday.nanostream.pipeline.LoopingTimerTransform;
import com.google.allenday.nanostream.pipeline.PipelineManagerService;
import com.google.allenday.nanostream.pipeline.SequenceOnlyDNACoder;
import com.google.allenday.nanostream.probecalculation.KVCalculationAccumulatorFn;
import com.google.allenday.nanostream.pubsub.DecodeNotificationJsonMessage;
import com.google.allenday.nanostream.pubsub.FilterObjectFinalizeMessage;
import com.google.allenday.nanostream.pubsub.GCSSourceData;
import com.google.allenday.nanostream.taxonomy.GetResistanceGenesTaxonomyDataFn;
import com.google.allenday.nanostream.taxonomy.GetTaxonomyFromTree;
import com.google.allenday.nanostream.util.CoderUtils;
import com.google.allenday.nanostream.util.trasform.FlattenMapToKV;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;

import java.io.Serializable;
import java.util.Map;

import static com.google.allenday.nanostream.ProcessingMode.RESISTANT_GENES;

public class NanostreamPipeline implements Serializable {

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

        Window<KV<KV<GCSSourceData, String>, GeneData>> globalWindowWithTriggering = Window
                .<KV<KV<GCSSourceData, String>, GeneData>>into(new GlobalWindows())
                .triggering(Repeatedly.forever(AfterProcessingTime
                        .pastFirstElementInPane()
                        .plusDelayOf(Duration.standardSeconds(options.getStatisticUpdatingDelay()))))
                .withAllowedLateness(Duration.ZERO)
                .accumulatingFiredPanes();

        pipeline.apply("Reading PubSub", PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInputDataSubscription()))
                .apply("Filter only ADD FILE", ParDo.of(new FilterObjectFinalizeMessage()))
                .apply("Deserialize messages", ParDo.of(new DecodeNotificationJsonMessage()))
                .apply("Parse GCloud notification", ParDo.of(injector.getInstance(ParseGCloudNotification.class)))
                .apply("Create FastQ batches", injector.getInstance(CreateBatchesTransform.class))
                .apply("Alignment", injector.getInstance(AlignTransform.class))
                .apply("Looping timer", new LoopingTimerTransform<>(
                        options.getAutoStopDelay(),
                        injector.getInstance(PipelineManagerService.class)))
                .apply("Extract Sequences",
                        ParDo.of(injector.getInstance(GetSequencesFromSamDataFn.class)))
                .apply("Remove Sequence part", MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptor.of(GCSSourceData.class), TypeDescriptors.strings()))
                        .via(KV::getKey))
                .apply("Get Taxonomy data", getTaxonomyData(pipeline))
                .apply("Global Window with Repeatedly triggering" + options.getStatisticUpdatingDelay(), globalWindowWithTriggering)
                .apply("Accumulate results to Map", Combine.globally(new KVCalculationAccumulatorFn()))
                .apply("Flatten result map", ParDo.of(new FlattenMapToKV<>()))
                .apply("Prepare sequences statistic to output",
                        ParDo.of(injector.getInstance(PrepareSequencesStatisticToOutputDbFn.class)))
                .apply("Write sequences statistic to Firestore",
                        ParDo.of(injector.getInstance(WriteDataToFirestoreDbFn.class)))
        ;

        pipeline.run();
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
}
