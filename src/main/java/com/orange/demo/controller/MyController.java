package com.orange.demo.controller;

import java.time.Duration;
import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.orange.demo.config.RedisListenerConfig;
import com.orange.demo.model.Profile;

/**
 * Copyright (c). All Rights Reserved.
 * 
 * This class
 * 
 * @author Abhishek Singhal [abhishek.singhal7795@gmail.com]
 *
 */
@RestController
public class MyController {

	@Autowired
	private RedisTemplate<String, String> redisTemplate;

	@Autowired
	private RedisListenerConfig redisListenerConfig;

	@Value("${redis.consumer.name}")
	private String consumer;

	@Value("${redis.consumer.group}")
	private String group;

	@Value("${redis.stream.key}")
	private String redisKey;

	// create a key
	@PostConstruct
	void init() {
		if (!redisTemplate.hasKey(redisKey)) {
			redisTemplate.opsForStream().createGroup(redisKey, group);
		}
	}

	@GetMapping(path = "save/{name}")
	public String hello(@PathVariable("name") String name) {
		Profile profile = new Profile(name, name + "@gmail.com");
		ObjectRecord<String, Profile> record = StreamRecords.newRecord().ofObject(profile).withStreamKey(redisKey);
		RecordId submittedRecord = redisTemplate.opsForStream().add(record);
		return submittedRecord.getValue();
	}

	@GetMapping(path = "pending")
	public List<PendingMessage> pending() {
		List<PendingMessage> messages = redisTemplate.opsForStream().pending(redisKey, Consumer.from(group, consumer))
				.toList();
		for (PendingMessage message : messages) {
			redisTemplate.getConnectionFactory().getConnection().xClaim(redisKey.getBytes(), group, consumer,
					Duration.ofMillis(100), message.getId());
		}
		return messages;
	}

	@GetMapping(path = "start")
	public void start() {
		redisListenerConfig.start();
	}

	@GetMapping(path = "stop")
	public void stop() {
		redisListenerConfig.stop();
	}

}
