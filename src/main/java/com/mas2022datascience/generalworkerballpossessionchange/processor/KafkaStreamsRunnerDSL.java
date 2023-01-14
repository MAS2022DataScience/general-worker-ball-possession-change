package com.mas2022datascience.tracabgen5spliter.processor;

import com.mas2022datascience.avro.v1.Object;
import com.mas2022datascience.avro.v1.PlayerBall;
import com.mas2022datascience.avro.v1.TracabGen5TF01;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsRunnerDSL {

  @Value(value = "${spring.kafka.properties.schema.registry.url}") private String schemaRegistry;
  @Value(value = "${topic.tracab-01.name}") private String topicIn;
  @Value(value = "${topic.general-01.name}") private String topicOutPlayer;
  @Value(value = "${topic.general-02.name}") private String topicOutTeam;

  @Bean
  public KStream<String, TracabGen5TF01> kStream(StreamsBuilder kStreamBuilder) {

    // When you want to override serdes explicitly/selectively
    final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
        schemaRegistry);
    final Serde<TracabGen5TF01> tracabGen5TF01Serde = new SpecificAvroSerde<>();
    tracabGen5TF01Serde.configure(serdeConfig, false); // `false` for record values

    KStream<String, TracabGen5TF01> stream = kStreamBuilder.stream(topicIn,
        Consumed.with(Serdes.String(), tracabGen5TF01Serde));

    KStream<String, PlayerBall> playerStream = stream.flatMap(
      (key, value) -> {
        List<KeyValue<String, PlayerBall>> result = new LinkedList<>();
        for ( Object valueObject : value.getObjects() ) {
          result.add(KeyValue.pair(
              key+"-"+valueObject.getId(),
              PlayerBall
                  .newBuilder()
                  .setId(valueObject.getId())
                  .setMatchId(key)
                  .setTs(Instant.ofEpochMilli(utcString2epocMs(value.getUtc())))
                  .setX(valueObject.getX())
                  .setY(valueObject.getY())
                  .setZ(valueObject.getZ())
                  .setVelocity(valueObject.getVelocity())
                  .setAccelleration(valueObject.getAccelleration())
                  .setDistance(valueObject.getDistance())
                  .build()
              )
          );
        }
        return result;
      }
    );
    playerStream.to(topicOutPlayer);

    //playerStream.filter((key, value) -> value.getId().equals("250101463")).to("temp");

    return stream;

  }

  /**
   * Converts the utc string of type "yyyy-MM-dd'T'HH:mm:ss.SSS" to epoc time in milliseconds.
   * @param utcString of type String of format "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
   * @return epoc time in milliseconds
   */
  private static long utcString2epocMs(String utcString) {
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        .withZone(ZoneOffset.UTC);

    return Instant.from(fmt.parse(utcString)).toEpochMilli();
  }
}


