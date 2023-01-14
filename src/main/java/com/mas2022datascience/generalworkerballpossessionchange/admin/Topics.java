package com.mas2022datascience.tracabgen5spliter.admin;

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

  @Value(value = "${topic.general-02.name}")
  private String topicNameTeam;
  @Value(value = "${topic.general-02.partitions}")
  private Integer topicPartitionsTeam;
  @Value(value = "${topic.general-02.replication-factor}")
  private Integer topicReplicationFactorTeam;

  // creates or alters the topic
  @Bean
  public NewTopic general02() {
    return TopicBuilder.name(topicNameTeam)
        .partitions(topicPartitionsTeam)
        .replicas(topicReplicationFactorTeam)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .build();
  }

//  @Bean
//  public NewTopic temp() {
//    return TopicBuilder.name("temp")
//        .partitions(3)
//        .replicas(2)
//        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
//        .build();
//  }

}