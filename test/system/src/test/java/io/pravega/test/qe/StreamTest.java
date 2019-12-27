/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.qe;

import io.pravega.controller.server.rest.generated.api.JacksonJsonProvider;
import io.pravega.controller.server.rest.generated.model.*;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.*;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import static javax.ws.rs.core.Response.Status.*;
import static org.junit.Assert.assertEquals;

@Slf4j
@RunWith(SystemTestRunner.class)
public class StreamTest extends AbstractSystemTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(5 * 60);

    private final Client client;
    private WebTarget webTarget;
    private String resourceURl;
    private static String restServerURI;
    private static Service conService;
    private static URI controllerRESTUri;
    private final String scopeName = RandomStringUtils.randomAlphanumeric(6);

    public StreamTest() {

        org.glassfish.jersey.client.ClientConfig clientConfig = new org.glassfish.jersey.client.ClientConfig();
        clientConfig.register(JacksonJsonProvider.class);
        clientConfig.property("sun.net.http.allowRestrictedHeaders", "true");

        if (Utils.AUTH_ENABLED) {
            HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic("admin", "1111_aaaa");
            clientConfig.register(feature);
        }

        client = ClientBuilder.newClient(clientConfig);
    }

    /**
     * This is used to setup the various services required by the system test framework.
     *
     * @throws InterruptedException If interrupted
     * @throws MarathonException    when error in setup
     * @throws URISyntaxException   If URI is invalid
     */
    @Environment
    public static void initialize() throws MarathonException {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = ensureControllerRunning(zkUri);
        ensureSegmentStoreRunning(zkUri, controllerUri);
    }

    @Before
    public void setup() {
        conService = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = conService.getServiceDetails();
        controllerRESTUri = ctlURIs.get(1);
        restServerURI = "http://" + controllerRESTUri.getHost() + ":" + controllerRESTUri.getPort();
        log.info("REST Server URI: {}", restServerURI);
    }

    // Test create stream with Alphanumeric name
    @Test
    public void createAlphaNumStream() {
        final String streamName = RandomStringUtils.randomAlphanumeric(6)+"12";
        Response response = createStream(scopeName,streamName);
        assertEquals("Create stream status", CREATED.getStatusCode(), response.getStatus());
        final StreamProperty streamPropertyResponse = response.readEntity(StreamProperty.class);
        assertEquals("Scope name in response", scopeName, streamPropertyResponse.getScopeName());
        assertEquals("Stream name in response", streamName, streamPropertyResponse.getStreamName());
        log.info("Create stream: {} successful", streamName);
    }

    // Test create a stream name with blank character
    @Test
    public void createBlankStream() {
        final String streamName = "";
        Response response = createStream(scopeName,streamName);
        assertEquals("Create stream status", INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        log.info("Stream name with blank character is not possible, which is expected one");
    }

    // Test create same Stream in different scope
    @Test
    public void sameStreamInDifScope() {
        final String streamName = RandomStringUtils.randomAlphanumeric(8);
        Response response = createStream(scopeName,streamName);
        assertEquals("Create stream status", CREATED.getStatusCode(), response.getStatus());
        final StreamProperty streamPropertyResponse = response.readEntity(StreamProperty.class);
        assertEquals("Scope name in response", scopeName, streamPropertyResponse.getScopeName());
        assertEquals("Stream name in response", streamName, streamPropertyResponse.getStreamName());

        String scope2 = RandomStringUtils.randomAlphanumeric(5);
        response = createStream(scope2,streamName);
        assertEquals("Create stream status", CREATED.getStatusCode(), response.getStatus());
        final StreamProperty streamPropertyResponse2 = response.readEntity(StreamProperty.class);
        assertEquals("Scope name in response", scope2, streamPropertyResponse2.getScopeName());
        assertEquals("Stream name in response", streamName, streamPropertyResponse2.getStreamName());

        log.info("Create stream: {} successful", streamName);
    }

    // Test a second Stream with same name in same Scope
    @Test
    public void createDuplicateStream() {
        final String streamName = RandomStringUtils.randomAlphanumeric(8);
        Response response = createStream(scopeName,streamName);
        assertEquals("Create stream status", CREATED.getStatusCode(), response.getStatus());
        final StreamProperty streamPropertyResponse = response.readEntity(StreamProperty.class);
        assertEquals("Scope name in response", scopeName, streamPropertyResponse.getScopeName());
        assertEquals("Stream name in response", streamName, streamPropertyResponse.getStreamName());

        response = createStream(scopeName,streamName);
        assertEquals("Create stream status", CONFLICT.getStatusCode(), response.getStatus());
        log.info("Not able to create duplicate stream in same scope, which is expected one");
    }

    // Test create multiple Streams
    @Test
    public void createMultipleStreams() {
        final String streamName1 = RandomStringUtils.randomAlphanumeric(6)+"34";
        Response response = createStream(scopeName,streamName1);
        assertEquals("Create stream status", CREATED.getStatusCode(), response.getStatus());
        final StreamProperty streamPropertyResponse = response.readEntity(StreamProperty.class);
        assertEquals("Scope name in response", scopeName, streamPropertyResponse.getScopeName());
        assertEquals("Stream name in response", streamName1, streamPropertyResponse.getStreamName());
        log.info("Create stream: {} successful", streamName1);

        final String streamName2 = RandomStringUtils.randomAlphanumeric(6)+"56";
        response = createStream(scopeName,streamName2);
        assertEquals("Create stream status", CREATED.getStatusCode(), response.getStatus());
        final StreamProperty streamPropertyResponse2 = response.readEntity(StreamProperty.class);
        assertEquals("Scope name in response", scopeName, streamPropertyResponse2.getScopeName());
        assertEquals("Stream name in response", streamName2, streamPropertyResponse2.getStreamName());
        log.info("Create stream: {} successful", streamName2);
    }

    // Test delete multiple Streams
    @Test
    public void deleteMultipleStreams() {
        final String streamName1 = RandomStringUtils.randomAlphanumeric(6)+"34";
        Response response = createStream(scopeName,streamName1);
        assertEquals("Create stream status", CREATED.getStatusCode(), response.getStatus());

        final String streamName2 = RandomStringUtils.randomAlphanumeric(6)+"56";
        response = createStream(scopeName,streamName2);
        assertEquals("Create stream status", CREATED.getStatusCode(), response.getStatus());

        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + scopeName + "/streams/" + streamName1).toString();
        response = client.target(resourceURl).request().delete();
        assertEquals("DeleteStream status", NO_CONTENT.getStatusCode(), response.getStatus());
        log.info("Delete stream: {} successful", streamName1);

        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + scopeName + "/streams/" + streamName2).toString();
        response = client.target(resourceURl).request().delete();
        assertEquals("DeleteStream status", NO_CONTENT.getStatusCode(), response.getStatus());
        log.info("Delete stream: {} successful", streamName2);
    }

    private Response createStream(String scopeName, String streamName){
        if(!isScopePresent(scopeName)){
            // create scope
            resourceURl = new StringBuilder(restServerURI).append("/v1/scopes").toString();
            webTarget = client.target(resourceURl);

            CreateScopeRequest createScopeRequest = new CreateScopeRequest();
            createScopeRequest.setScopeName(scopeName);
            Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
            Response response = builder.post(Entity.json(createScopeRequest));

            assertEquals("Create scope status", CREATED.getStatusCode(), response.getStatus());
            log.info("Create scope: {} successful ", scopeName);
        }
        // create stream
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + scopeName + "/streams").toString();
        webTarget = client.target(resourceURl);

        CreateStreamRequest createStreamRequest = new CreateStreamRequest();
        ScalingConfig scalingConfig = new ScalingConfig();
        scalingConfig.setType(ScalingConfig.TypeEnum.FIXED_NUM_SEGMENTS);
        scalingConfig.setTargetRate(2);
        scalingConfig.scaleFactor(2);
        scalingConfig.minSegments(2);

        RetentionConfig retentionConfig = new RetentionConfig();
        retentionConfig.setType(RetentionConfig.TypeEnum.LIMITED_DAYS);
        retentionConfig.setValue(123L);

        createStreamRequest.setStreamName(streamName);
        createStreamRequest.setScalingPolicy(scalingConfig);
        createStreamRequest.setRetentionPolicy(retentionConfig);

        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        Response createStreamResponse = builder.post(Entity.json(createStreamRequest));

        return createStreamResponse;

    }

    private boolean isScopePresent(String scopeName){
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + scopeName).toString();
        Response response = client.target(resourceURl).request().get();
        boolean isScopePresent = response.getStatus()==OK.getStatusCode()?true:false;
        return isScopePresent;
    }



}
