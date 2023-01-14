package com.mas2022datascience.generalworkerballpossessionchange.admin;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Component;

@Component
public class Topics {

  @Value(value = "${topic.tracab-01.name}")
  private String topicName1;
  @Value(value = "${topic.tracab-01.partitions}")
  private Integer topicPartitions1;
  @Value(value = "${topic.tracab-01.replication-factor}")
  private Integer topicReplicationFactor1;

  // creates or alters the topic
  @Bean
  public NewTopic tracab01() {
    return TopicBuilder.name(topicName1)
        .partitions(topicPartitions1)
        .replicas(topicReplicationFactor1)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .build();
  }

  @Value(value = "${topic.general-01.name}")
  private String topicNamePlayerBall;
  @Value(value = "${topic.general-01.partitions}")
  private Integer topicPartitionsPlayerBall;
  @Value(value = "${topic.general-01.replication-factor}")
  private Integer topicReplicationFactorPlayerBall;

  // creates or alters the topic
  @Bean
  public NewTopic general01() {
    return TopicBuilder.name(topicNamePlayerBall)
        .partitions(topicPartitionsPlayerBall)
        .replicas(topicReplicationFactorPlayerBall)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .build();
  }
}