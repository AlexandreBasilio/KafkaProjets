package com.example.barah.credicard.ccproducer.service;

import com.example.barah.credicard.ccproducer.common.AppConstants;
import com.example.barah.credicard.ccproducer.model.CreditCard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class CredicardService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final Logger logger =
            LoggerFactory.getLogger(CredicardService.class);

    @Autowired
    public CredicardService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendCreditCardData(CreditCard creditCard) {
        logger.info(String.format("service send cc %sto TOPIC ", creditCard.getCardNumber()));
        kafkaTemplate.send(AppConstants.CREDITCARD_TOPIC, creditCard);
    }
}
