package com.kafka.consumer.listeners;

import com.kafka.consumer.custom.ConsumerCustomListener;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ConsumerListener {

    @ConsumerCustomListener(groupId = "group-0")
    public void create(String message){
      log.info("Grupo de consumo 0 ::: \n{}", message);
    }

    @KafkaListener(
            groupId = "group-1",
            topicPartitions = {
                    @TopicPartition(
                            topic = "topico-v1",
                            partitions = {"0"}
                    )
            },
            containerFactory = "containerFactory"
    )
    public void info(String message){
        log.info("Grupo de consumo 1 - Particao 0 ::: \n{}", message);
    }

    @KafkaListener(
            groupId = "group-1",
            topicPartitions = {
                    @TopicPartition(
                            topic = "topico-v1",
                            partitions = {"1"}
                    )
            },
            containerFactory = "containerFactory"
    )
    public void log(String message){
        log.info("Grupo de consumo 1 - Particao 1 ::: \n{}", message);
    }

    @KafkaListener(
            groupId = "group-2",
            topics = "topico-v1",
            containerFactory = "containerFactory"
    )
    public void history(String message){
        log.info("Grupo de consumo 2 ::: \n{}", message);
    }

    @KafkaListener(
            groupId = "group-2",
            topics = "topico-v1",
            containerFactory = "validMessageContainerFactory"
    )
    public void validMessage(String message){
        log.info("Grupo de consumo validMessageContainerFactory ::: \n{}", message);
    }

    @SneakyThrows
    @ConsumerCustomListener(groupId = "group-0")
    public void exception(String message){
        log.info("Grupo de consumo Exception ::: {}", message.trim());
        throw new IllegalArgumentException("ERRO");
    }
}
