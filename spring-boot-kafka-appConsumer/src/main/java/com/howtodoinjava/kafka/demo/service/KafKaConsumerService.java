package com.howtodoinjava.kafka.demo.service;

import com.alex.spring.boot.kafka.demo.model.UserAlex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.howtodoinjava.kafka.demo.common.AppConstants;
import com.howtodoinjava.kafka.demo.model.User;

@Service
public class KafKaConsumerService 
{
	private final Logger logger
		= LoggerFactory.getLogger(KafKaConsumerService.class);

	@KafkaListener(topics = AppConstants.TOPIC_NAME_TEST, groupId = AppConstants.GROUP_ID)
	public void consume(String message) {
		logger.info(String.format("LOG consummer Message recieved -> %s", message));
		System.out.println("PRINTMESSAGE=" + message);
	}

	@KafkaListener(topics = AppConstants.TOPIC_NAME_USER_LOG, groupId = AppConstants.GROUP_ID)
	public void consume(User user) {
		logger.info(String.format("LOG consummer User created -> %s", user));
		System.out.println("PRINTUSER=" + user.getFirstName());
	}

	@KafkaListener(topics = AppConstants.TOPIC_NAME_USER_PARM_ONLY_LOG, groupId = AppConstants.GROUP_ID)
	public void consume(int age) {
		logger.info(String.format("LOG consummer User AGEeeeee -> %s", age));
		System.out.println("PRINTUSER=" + age);
	}

	@KafkaListener(topics = "user-topic-new", groupId = AppConstants.GROUP_ID)
	public void consume(UserAlex userAlex) {
		logger.info(String.format("LOG consummer User created -> %s", userAlex));
		System.out.println("PRINTUSER AGE=" + userAlex.getAge());
	}
}
