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
import org.junit.*;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

import static javax.ws.rs.core.Response.Status.*;
import static org.junit.Assert.assertEquals;

@Slf4j
//@RunWith(SystemTestRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ExternalConnectionTest extends AbstractSystemTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(5 * 60);

    private final Client client;
    private WebTarget webTarget;
    private String resourceURl;
    private static String restServerURI;
    private static Service conService;
    private static URI controllerRESTUri;
    private static final String scopeName = "ScopeForScopeStreamBasicTest";
    private static final String streamName = "StreamForScopeStreamBasicTest";

    public ExternalConnectionTest() {

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
        List<URI> ctlURIs = conService.getExternalServiceDetails();
        controllerRESTUri = ctlURIs.get(1);
        restServerURI = "http://" + controllerRESTUri.getHost() + ":" + controllerRESTUri.getPort();
        log.info("REST Server URI: {}", restServerURI);
    }

    @Test
    public void scopeStreamBasicTests(){
        test1_createScope();
        test2_getDetailsOfScope();
        test3_createStream();
        test4_getDetailsOfStream();
        test5_updateStreamState();
        test6_deleteStream();
        test7_deleteScope();
    }

    private void test1_createScope() {
        CreateScopeRequest createScopeRequest = new CreateScopeRequest();
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes").toString();
        webTarget = client.target(resourceURl);

        createScopeRequest.setScopeName(scopeName);
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        Response response = builder.post(Entity.json(createScopeRequest));

        assertEquals("Create scope status", CREATED.getStatusCode(), response.getStatus());
        Assert.assertEquals("Create scope response", scopeName, response.readEntity(ScopeProperty.class).getScopeName());
        log.info("Create scope: {} successful ", scopeName);
    }

    private void test2_getDetailsOfScope() {
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + scopeName).toString();
        Response response = client.target(resourceURl).request().get();
        assertEquals("Get scope status", OK.getStatusCode(), response.getStatus());
        assertEquals("Get scope response", scopeName, response.readEntity(ScopeProperty.class).getScopeName());
        log.info("Get scope successful");
    }

    private void test3_createStream() {
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
        Response response = builder.post(Entity.json(createStreamRequest));

        assertEquals("Create stream status", CREATED.getStatusCode(), response.getStatus());
        final StreamProperty streamPropertyResponse = response.readEntity(StreamProperty.class);
        assertEquals("Scope name in response", scopeName, streamPropertyResponse.getScopeName());
        assertEquals("Stream name in response", streamName, streamPropertyResponse.getStreamName());
        log.info("Create stream: {} successful", streamName);
    }

    private void test4_getDetailsOfStream() {
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + scopeName + "/streams").toString();
        webTarget = client.target(resourceURl);
        Invocation.Builder builder = webTarget.request();
        Response response = builder.get();
        assertEquals("List streams", OK.getStatusCode(), response.getStatus());
        Assert.assertEquals("List streams size", 1, response.readEntity(StreamsList.class).getStreams().size());
        log.info("List streams successful");
    }

    private void test5_updateStreamState() {
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + scopeName + "/streams/" + streamName + "/state")
                .toString();
        StreamState streamState = new StreamState();
        streamState.setStreamState(StreamState.StreamStateEnum.SEALED);
        Response response = client.target(resourceURl).request(MediaType.APPLICATION_JSON_TYPE)
                .put(Entity.json(streamState));
        assertEquals("UpdateStreamState status", OK.getStatusCode(), response.getStatus());
        assertEquals("UpdateStreamState status in response", streamState.getStreamState(),
                response.readEntity(StreamState.class).getStreamState());
        log.info("Update stream state successful");
    }

    private void test6_deleteStream() {
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + scopeName + "/streams/" + streamName)
                .toString();
        Response response = client.target(resourceURl).request().delete();
        assertEquals("DeleteStream status", NO_CONTENT.getStatusCode(), response.getStatus());
        log.info("Delete stream successful");
    }

    private void test7_deleteScope() {
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + scopeName).toString();
        Response response = client.target(resourceURl).request().delete();
        assertEquals("Get scope status", NO_CONTENT.getStatusCode(), response.getStatus());
        log.info("Delete Scope successful");
    }

}
