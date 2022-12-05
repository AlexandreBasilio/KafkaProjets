package com.howtodoinjava.kafka.demo.service;

import com.alex.spring.boot.kafka.demo.model.UserAlex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.howtodoinjava.kafka.demo.common.AppConstants;
import com.howtodoinjava.kafka.demo.model.User;

@Service
public class KafKaProducerService 
{
	private static final Logger logger = 
			LoggerFactory.getLogger(KafKaProducerService.class);
	
	@Autowired
	private KafkaTemplate<String, Object> kafkaTemplate;

	public void sendMessage(String message) 
	{
		logger.info(String.format("LOG Message sent -> %s", message));
		this.kafkaTemplate.send(AppConstants.TOPIC_NAME_TEST, message);
	}
	
	public void saveCreateUserLogFromControllerRequestParam(User user)
	{
		logger.info(String.format("User by RequestParam created -> %s", user));
		this.kafkaTemplate.send(AppConstants.TOPIC_NAME_USER_LOG, user);
	}

	public void saveCreateUserLogFromControllerUserWithParamsOnly(String userId, String firstName, int age)
	{
		logger.info(String.format("User WithParamsOnly createddddd -> %s", firstName));
		this.kafkaTemplate.send(AppConstants.TOPIC_NAME_USER_PARM_ONLY_LOG, userId, age);

	}

	public void saveCreateUserLogFromControllerRequestBody(User user)
	{
		logger.info(String.format("User by RequestBody created -> %s", user));
		this.kafkaTemplate.send(AppConstants.TOPIC_NAME_USER_LOG, user);
	}

	public void saveCreateUserAlexLogFromControllerRequestBody(UserAlex userAlex)
	{
		logger.info(String.format("User ALEX by RequestBody created -> %s", userAlex));
		this.kafkaTemplate.send("user-topic-new", userAlex);
	}
}
