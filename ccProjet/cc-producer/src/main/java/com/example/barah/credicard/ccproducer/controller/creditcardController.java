package com.example.barah.credicard.ccproducer.controller;

import com.example.barah.credicard.ccproducer.model.CreditCard;
import com.example.barah.credicard.ccproducer.service.CredicardService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/creditCard")
public class creditcardController {

    private static final Logger logger = LoggerFactory.getLogger(creditcardController.class);

    @Autowired
    private CredicardService credicardService;

    @PostMapping("/send")
    public void sendCreditCardData(@RequestBody CreditCard creditCard)  {
        logger.info(String.format("controller send cc %sto TOPIC ", creditCard.getCardNumber()));
        credicardService.sendCreditCardData(creditCard);
    }
}
