package com.example.dsw.kafka.streams.pageview;

import com.example.dsw.kafka.streams.JSONSerDe;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class PageViewTypedGender {

    public static void main(final String[] args) {

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dsw-kafka-streams" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, JSONSerDe.class);
        props.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, JSONSerDe.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerDe.class);
        props.put(StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, JSONSerDe.class);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); //"earliest");

        PageViewTypedGender app = new PageViewTypedGender();

        Topology topology = app.buildTopology();

        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("pipe-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }


    private Topology buildTopology() {

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, POJO.PageView> views = builder.stream("pageview", Consumed.with(Serdes.String(), new PageViewTypedDemo.JSONSerde<>()));

        final KTable<String, POJO.User> users = builder.table("userprofile", Consumed.with(Serdes.String(), new PageViewTypedDemo.JSONSerde<>()));

        final KStream<POJO.WindowedPageViewByGender, POJO.GenderCount> genderCountStream
                = views.leftJoin(users, (view, profile) ->
                        new POJO.PageViewByGender(view.userid, view.pageid
                                , null != profile ? profile.gender : "UNKNOWN"))
                .map((user, viewGender) -> new KeyValue<>(viewGender.gender, viewGender))
                .groupByKey(Grouped.with(Serdes.String(), new JSONSerDe<>()))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).advanceBy(Duration.ofSeconds(10)))
                .count()
                .toStream()
                .map((key, value) -> {
                    POJO.WindowedPageViewByGender windowedPageViewByGender = new POJO.WindowedPageViewByGender();
                    windowedPageViewByGender.gender = key.key();
                    windowedPageViewByGender.windowStart = key.window().start();
                    windowedPageViewByGender.windowStartString = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss").format(new Date(key.window().start()));

                    POJO.GenderCount genderCount = new POJO.GenderCount();
                    genderCount.count = value;
                    genderCount.gender = key.key();

                    return new KeyValue<>(windowedPageViewByGender, genderCount);
                });
        genderCountStream.to("pageview-gender-stats");
        return builder.build();
    }

}
