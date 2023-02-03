package com.example.controller;

import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;

import org.json.simple.JSONObject;

import java.util.HashMap;

@Controller("/test")
public class HomeController {
    private HashMap<String, JSONObject> tweets = new HashMap<>();



    @Post(produces = MediaType.TEXT_PLAIN)
    public void receiveTweets(HashMap<String,String> tweets) {
        System.out.println("DATA RECEIVED"+tweets);
        //System.out.println(tweets.size());
    }
}