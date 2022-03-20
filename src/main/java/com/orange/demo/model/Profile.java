package com.orange.demo.model;

/**
 * Copyright (c). All Rights Reserved.
 * 
 * This class
 * 
 * @author Abhishek Singhal [abhishek.singhal7795@gmail.com]
 *
 */
public class Profile {

	private String name;
	private String email;

	public Profile() {
	}

	public Profile(String name, String email) {
		this.name = name;
		this.email = email;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	@Override
	public String toString() {
		return "Profile [name=" + name + ", email=" + email + "]";
	}

}
