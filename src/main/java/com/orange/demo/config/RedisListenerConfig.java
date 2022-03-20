package com.orange.demo.config;

import java.net.UnknownHostException;
import java.time.Duration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import com.orange.demo.model.Profile;

/**
 * Copyright (c). All Rights Reserved.
 * 
 * This class
 * 
 * @author Abhishek Singhal [abhishek.singhal7795@gmail.com]
 *
 */
@Configuration
public class RedisListenerConfig {

	@Value("${redis.consumer.name}")
	private String consumer;

	@Value("${redis.consumer.group}")
	private String group;

	@Value("${redis.stream.key}")
	private String redisKey;

	@Autowired
	private StreamListener<String, ObjectRecord<String, Profile>> streamListener;

	StreamMessageListenerContainer<String, ObjectRecord<String, Profile>> listenerContainer;

	@Bean
	public Subscription subscription(RedisConnectionFactory redisConnectionFactory) throws UnknownHostException {
		StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, ObjectRecord<String, Profile>> options = StreamMessageListenerContainer.StreamMessageListenerContainerOptions
				.builder().pollTimeout(Duration.ofSeconds(1)).targetType(Profile.class).build();

		listenerContainer = StreamMessageListenerContainer.create(redisConnectionFactory, options);

		Subscription subscription = listenerContainer.receive(Consumer.from(group, consumer),
				StreamOffset.create(redisKey, ReadOffset.lastConsumed()), streamListener);
		listenerContainer.start();
		return subscription;
	}

	public void stop() {
		listenerContainer.stop();
	}

	public void start() {
		listenerContainer.start();
	}
}
