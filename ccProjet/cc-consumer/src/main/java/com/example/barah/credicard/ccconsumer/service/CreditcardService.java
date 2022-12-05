package com.example.barah.credicard.ccconsumer.service;

import com.example.barah.credicard.ccconsumer.common.AppConstants;
import com.example.barah.credicard.ccproducer.model.CreditCard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class CreditcardService {

    private static final Logger logger = LoggerFactory.getLogger(CreditcardService.class);

    @KafkaListener (topics = AppConstants.CREDITCARD_TOPIC)
    public void consumeCreditcard (CreditCard creditCard) {
        logger.info(String.format("Creditcard card %s consumido", creditCard.getCardNumber()));
    }
}
