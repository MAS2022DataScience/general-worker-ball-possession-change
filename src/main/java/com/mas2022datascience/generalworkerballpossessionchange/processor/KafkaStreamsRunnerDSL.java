package com.mas2022datascience.generalworkerballpossessionchange.processor;

import com.mas2022datascience.avro.v1.GeneralBallPossessionChange;
import com.mas2022datascience.avro.v1.GeneralMatchTeam;
import com.mas2022datascience.avro.v1.Object;
import com.mas2022datascience.avro.v1.TracabGen5TF01;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsRunnerDSL {

  @Value(value = "${spring.kafka.properties.schema.registry.url}") private String schemaRegistry;
  @Value(value = "${topic.tracab-01.name}") private String topicIn;
  @Value(value = "${topic.general-01.name}") private String topicOutBallPossessionChange;
  @Value(value = "${topic.general-match-team.name}") private String topicGeneralMatchTeam;

  @Bean
  public KStream<String, TracabGen5TF01> kStream(StreamsBuilder kStreamBuilder) {

    // When you want to override serdes explicitly/selectively
    final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
        schemaRegistry);
    final Serde<TracabGen5TF01> tracabGen5TF01Serde = new SpecificAvroSerde<>();
    tracabGen5TF01Serde.configure(serdeConfig, false); // `false` for record values

    final Serde<GeneralBallPossessionChange> generalBallPossessionChangeSerde = new
        SpecificAvroSerde<>();
    generalBallPossessionChangeSerde.configure(serdeConfig, false);//`false` for record values

    final Serde<GeneralMatchTeam> generalMatchTeamSerde = new SpecificAvroSerde<>();
    generalMatchTeamSerde.configure(serdeConfig, false); // `false` for record values

    KStream<String, GeneralMatchTeam> streamTeam = kStreamBuilder.stream(topicGeneralMatchTeam,
        Consumed.with(Serdes.String(), generalMatchTeamSerde));

    KTable<String, GeneralMatchTeam> teams = streamTeam
        .groupByKey(Grouped.with(Serdes.String(), generalMatchTeamSerde))
        .reduce((oldEvent, newEvent) -> newEvent,
            Materialized.as("general-match-team-store"));

    KStream<String, TracabGen5TF01> stream = kStreamBuilder.stream(topicIn,
        Consumed.with(Serdes.String(), tracabGen5TF01Serde));

    final StoreBuilder<KeyValueStore<String, TracabGen5TF01>> myStateStore = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore("MyTracabGen5StateStore"),
            Serdes.String(), tracabGen5TF01Serde);
    kStreamBuilder.addStateStore(myStateStore);

    final MyTracabGen5StateHandler myStateHandler =
        new MyTracabGen5StateHandler(myStateStore.name());

    // invoke the transformer
    KStream<String, TracabGen5TF01> transformedStream = stream
        .transform(() -> myStateHandler, myStateStore.name());

    transformedStream
        .filter((key, values) -> values != null)
        .map((key, value) -> {
          return new KeyValue<String, GeneralBallPossessionChange>(key,
              GeneralBallPossessionChange.newBuilder()
                  .setTs(Instant.ofEpochMilli(utcString2epocMs(value.getUtc())))
                  .setMatchId(key)
                  .setLostPossessionTeamId(value.getBallPossession().equals("A") ? "HOME" : "AWAY")
                  .setWonPossessionTeamId(value.getBallPossession().equals("H") ? "HOME" : "AWAY")
                  .setBallX(getBallObject(value.getObjects()).getX())
                  .setBallY(getBallObject(value.getObjects()).getY())
                  .build());
        })
        .leftJoin(teams, (newValue, teamsValue) -> {
          if (newValue.getWonPossessionTeamId().equals("HOME")) {
            newValue.setWonPossessionTeamId(String.valueOf(teamsValue.getHomeTeamID()));
            newValue.setLostPossessionTeamId(String.valueOf(teamsValue.getAwayTeamID()));
          } else {
            newValue.setWonPossessionTeamId(String.valueOf(teamsValue.getAwayTeamID()));
            newValue.setLostPossessionTeamId(String.valueOf(teamsValue.getHomeTeamID()));
          }
          return newValue;
        })
        .to(topicOutBallPossessionChange, Produced.with(Serdes.String(),
            generalBallPossessionChangeSerde));
//        .print(Printed.<String, GeneralBallPossessionChange>toSysOut());

    return stream;

  }

  private static final class MyTracabGen5StateHandler implements
      Transformer<String, TracabGen5TF01, KeyValue<String, TracabGen5TF01>> {
    final private String storeName;
    private KeyValueStore<String, TracabGen5TF01> stateStore;

    public MyTracabGen5StateHandler(final String storeName) {
      this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext processorContext) {
      stateStore = processorContext.getStateStore(storeName);
    }

    @Override
    public KeyValue<String, TracabGen5TF01> transform(String key, TracabGen5TF01 value) {
      try {
        if (stateStore.get(key) == null) {
          stateStore.put(key, value);
          return new KeyValue<>(key, stateStore.get(key));
        }
      } catch (org.apache.kafka.common.errors.SerializationException ex) {
        // the first time the state store is empty, so we get a serialization exception
        stateStore.put(key, value);
        return new KeyValue<>(key, stateStore.get(key));
      } catch (Exception e) {
        e.printStackTrace();
        return null;
      }

      TracabGen5TF01 oldFrame = stateStore.get(key);

      if (oldFrame.getIsBallInPlay().equals("Alive") && value.getIsBallInPlay().equals("Alive")) {
        if (!oldFrame.getBallPossession().equals(value.getBallPossession())) {
          stateStore.put(key, value);
          return new KeyValue<>(key, stateStore.get(key));
        } else {
          stateStore.put(key, value);
          return new KeyValue<>(key, null);
        }
      } else {
        stateStore.put(key, value);
        return new KeyValue<>(key, null);
      }
    }

    @Override
    public void close() { }
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

  /**
   * Gets the ball object from the list of objects.
   * @param objects list of objects
   * @return the ball object
   */
  private Object getBallObject(List<Object> objects) {
    for (Object object : objects) {
      if (object.getType() == 7) {
        return object;
      }
    }
    return null;
  }
}


