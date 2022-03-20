package com.orange.demo.listener;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

import com.orange.demo.model.Profile;

/**
 * Copyright (c). All Rights Reserved.
 * 
 * This class
 * 
 * @author Abhishek Singhal [abhishek.singhal7795@gmail.com]
 *
 */
@Component
public class ProfileListener implements StreamListener<String, ObjectRecord<String, Profile>> {

	@Value("${redis.consumer.group}")
	private String group;

	@Value("${redis.stream.key}")
	private String redisKey;

	@Autowired
	private RedisTemplate<String, String> redisTemplate;

	@Override
	public void onMessage(ObjectRecord<String, Profile> message) {
		System.out.println(message.getValue());
		redisTemplate.opsForStream().acknowledge(redisKey, group, message.getId());
	}

}
