package com.kafka.consumer.listeners;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ConsumerListener {

    @KafkaListener(
            groupId = "group-1",
            topics = "topico-v1",
            containerFactory = "containerFactory"
    )
    public void listener(String message){
      log.info("Mensagem recebida: \n{}", message);
    }
}
