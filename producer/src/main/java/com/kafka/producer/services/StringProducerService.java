package com.kafka.producer.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class StringProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String message){

        kafkaTemplate
                .send("topico-v1", message)
                .whenComplete((result, e) -> {
                    if (e != null)
                        log.error("Mensagem não enviada: {}", e.getMessage());
                    else {
                        log.info("Mensagem enviada com sucesso para o tópico: {}", result.getRecordMetadata().topic());
                        log.info("Partição: {}", result.getRecordMetadata().partition());
                        log.info("Offset: {}", result.getRecordMetadata().offset());
                    }
                });
    }
}
