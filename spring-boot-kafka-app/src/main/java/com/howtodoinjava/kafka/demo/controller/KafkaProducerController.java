package com.howtodoinjava.kafka.demo.controller;

import com.alex.spring.boot.kafka.demo.model.UserAlex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import com.howtodoinjava.kafka.demo.model.User;
import com.howtodoinjava.kafka.demo.service.KafKaProducerService;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaProducerController {
	private final KafKaProducerService producerService;

	@Autowired
	public KafkaProducerController(KafKaProducerService producerService) {
		this.producerService = producerService;
	}

	@PostMapping(value = "/publish")
	public void sendMessageToKafkaTopic(@RequestParam("message") String message) {
		this.producerService.sendMessage(message);
	}
	
	@PostMapping(value = "/createUser")
	public void sendMessageToKafkaTopic(
			@RequestParam("userId") long userId, 
			@RequestParam("firstName") String firstName,
			@RequestParam("lastName") String lastName) {
		
		User user = new User();
		user.setUserId(userId);
		user.setFirstName(firstName);
		user.setLastName(lastName);
		
		this.producerService.saveCreateUserLogFromControllerRequestParam(user);
	}

	@PostMapping(value = "/UserWithParamsOnly")
	public void sendMessageToKafkaTopic(
			@RequestParam("userId") String userId,
			@RequestParam("firstName") String firstName,
			@RequestParam("age") int age) {

		this.producerService.saveCreateUserLogFromControllerUserWithParamsOnly(userId, firstName, age);
	}

	@PostMapping(value = "/createUserWithBoby")
	public void sendMessageToKafkaTopic(@RequestBody User user) {

		this.producerService.saveCreateUserLogFromControllerRequestBody(user);
	}

	@PostMapping(value = "/createUserAlexWithBoby")
	public void sendMessageToKafkaTopic(@RequestBody UserAlex userAlex) {

		this.producerService.saveCreateUserAlexLogFromControllerRequestBody(userAlex);
	}

}