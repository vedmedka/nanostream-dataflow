package com.google.allenday.nanostream;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class of the Nanostream Dataflow App that provides dataflow pipeline
 * with transformation from PubsubMessage to Sequences Statistic and Sequences Bodies
 */
public class NanostreamApp {

    private static final Logger logger = LoggerFactory.getLogger(NanostreamApp.class);

    public static void main(String[] args) {
        NanostreamPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(NanostreamPipelineOptions.class);

        new NanostreamPipeline(options).run();

//
//// We will start our timer at 1 sec from the fixed upper boundary of our
//        // minute window
//        Instant now = Instant.parse("2000-01-01T00:00:59Z");
//
//        // ----- Create some dummy data
//
//        // Create 3 elements, incrementing by 1 minute and leaving a time gap between
//        // element 2 and element 3
//        TimestampedValue<KV<String, Integer>> time_1 =
//                TimestampedValue.of(KV.of("Key_A", 1), now);
//
//        TimestampedValue<KV<String, Integer>> time_2 =
//                TimestampedValue.of(KV.of("Key_A", 2),
//                        now.plus(Duration.standardMinutes(1)));
//
//        // No Value for start time + 2 mins
//        TimestampedValue<KV<String, Integer>> time_3 =
//                TimestampedValue.of(KV.of("Key_A", 3),
//                        now.plus(Duration.standardMinutes(3)));
//
//        // Create pipeline
//        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
//                .as(PipelineOptions.class);
//
//        Pipeline p = Pipeline.create(options);
//
////        // Apply a fixed window of duration 1 min and Sum the results
////        Create.TimestampedValues<KV<String, Integer>> timestamped = Create.timestamped(time_1, time_2, time_3);
////        p.apply(timestamped)
////                .apply(
////                        Window.<KV<String,Integer>>into(
////                                FixedWindows.<Integer>of(Duration.standardMinutes(1))))
////                .apply(Sum.integersPerKey())
////                .apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {
////
////                    @ProcessElement public void process(ProcessContext c) {
////                        logger.info("Value is {} timestamp is {}", c.element(), c.timestamp());
////                    }
////                }));
//
//        // Apply a fixed window of duration 1 min and Sum the results
//        p.apply(Create.timestamped(time_1, time_2, time_3))
////                .apply(Window.<KV<String, Integer>>into(FixedWindows.<Integer>of(Duration.standardMinutes(1))))
////                // We use a combiner to reduce the number of calls in keyed state
////                // from all elements to 1 per FixedWindow
////                .apply(Sum.integersPerKey())
////                .apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {
////
////                    @ProcessElement
////                    public void process(ProcessContext c) {
////
////                        logger.info("1 -- Value is {} timestamp is {}", c.element(), c.timestamp());
////
////                    }
////                }))
//                .apply(Window.into(new GlobalWindows()))
//                .apply(ParDo.of(new LoopingStatefulTimer(Instant.parse("2000-01-01T00:04:00Z"))))
//                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
//                .apply(Sum.integersPerKey())
//                .apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {
//
//                    @ProcessElement public void process(ProcessContext c) {
//
//                        logger.info("2 -- Value is {} timestamp is {}", c.element(), c.timestamp());
//
//                    }
//                }));
//
//        p.run();
    }


    public static class LoopingStatefulTimer extends DoFn<KV<String, Integer>, KV<String, Integer>> {

        Instant stopTimerTime;

        LoopingStatefulTimer(Instant stopTime){
            this.stopTimerTime = stopTime;
        }

        @StateId("loopingTimerTime")
        private final StateSpec<ValueState<Long>> loopingTimerTime =
                StateSpecs.value(BigEndianLongCoder.of());

        @StateId("key")
        private final StateSpec<ValueState<String>> key =
                StateSpecs.value(StringUtf8Coder.of());

        @TimerId("loopingTimer")
        private final TimerSpec loopingTimer =
                TimerSpecs.timer(TimeDomain.EVENT_TIME);

        @ProcessElement public void process(ProcessContext c, @StateId("key") ValueState<String> key,
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
            }

            // We need this value so that we can output a value for the correct key in OnTimer
            if (key.read() == null) {
                key.write(c.element().getKey());
            }

            c.output(c.element());
        }

        @OnTimer("loopingTimer")
        public void onTimer(
                OnTimerContext c,
                @StateId("key") ValueState<String> key,
                @TimerId("loopingTimer") Timer loopingTimer) {

            logger.info("Timer @ {} fired", c.timestamp());
            c.output(KV.of(key.read(), 0));

            // If we do not put in a “time to live” value, then the timer would loop forever
            Instant nextTimer = c.timestamp().plus(Duration.standardMinutes(1));
            if (nextTimer.isBefore(stopTimerTime)) {
                loopingTimer.set(nextTimer);
            } else {
                logger.info(
                        "Timer not being set as exceeded Stop Timer value {} ",
                        stopTimerTime);
            }
        }
    }

}
