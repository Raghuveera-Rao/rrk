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
import io.pravega.controller.server.rest.generated.model.CreateScopeRequest;
import io.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import io.pravega.controller.server.rest.generated.model.RetentionConfig;
import io.pravega.controller.server.rest.generated.model.ScalingConfig;
import io.pravega.controller.server.rest.generated.model.ScopeProperty;
import io.pravega.controller.server.rest.generated.model.StreamProperty;
import io.pravega.controller.server.rest.generated.model.StreamState;
import io.pravega.controller.server.rest.generated.model.StreamsList;
import io.pravega.controller.server.rest.generated.model.UpdateStreamRequest;
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
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import static javax.ws.rs.core.Response.Status.*;
import static org.junit.Assert.assertEquals;

@Slf4j
@RunWith(SystemTestRunner.class)
public class ScopeTest extends AbstractSystemTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(5 * 60);

    private final Client client;
    private WebTarget webTarget;
    private String restServerURI;
    private String resourceURl;

    public ScopeTest() {

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

    @Test
    public void ScopeRelatedTests() {

        Service conService = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = conService.getServiceDetails();
        URI controllerRESTUri = ctlURIs.get(1);
        Invocation.Builder builder;
        Response response;

        restServerURI = "http://" + controllerRESTUri.getHost() + ":" + controllerRESTUri.getPort();
        log.info("REST Server URI: {}", restServerURI);

        // TEST REST server status, ping test
        resourceURl = new StringBuilder(restServerURI).append("/ping").toString();
        webTarget = client.target(resourceURl);
        builder = webTarget.request();
        response = builder.get();
        assertEquals("Ping test", OK.getStatusCode(), response.getStatus());
        log.info("REST Server is running. Ping successful.");

        // Test _system scope is present by default
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + "_system").toString();
        response = client.target(resourceURl).request().get();
        assertEquals("Get scope status", OK.getStatusCode(), response.getStatus());
        assertEquals("Get scope _system response", "_system", response.readEntity(ScopeProperty.class).getScopeName());
        log.info("_system scope is present");

        // Test delete _system scope
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + "_system").toString();
        response = client.target(resourceURl).request().delete();
        assertEquals("Delete scope _system status", PRECONDITION_FAILED.getStatusCode(), response.getStatus());
        log.info("Delete scope _system not possible, which is expected");

        CreateScopeRequest createScopeRequest = new CreateScopeRequest();
        String scopeName = RandomStringUtils.randomAlphanumeric(10);

        // TEST CreateScope POST http://controllerURI:Port/v1/scopes
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes").toString();
        webTarget = client.target(resourceURl);

        createScopeRequest.setScopeName(scopeName);
        builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        response = builder.post(Entity.json(createScopeRequest));

        assertEquals("Create scope status", CREATED.getStatusCode(), response.getStatus());
        Assert.assertEquals("Create scope response", scopeName, response.readEntity(ScopeProperty.class).getScopeName());
        log.info("Create scope: {} successful ", scopeName);

        // Test create scope containing number
        scopeName = RandomStringUtils.randomAlphanumeric(4);
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes").toString();
        webTarget = client.target(resourceURl);

        createScopeRequest = new CreateScopeRequest();
        createScopeRequest.setScopeName(scopeName);
        builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        response = builder.post(Entity.json(createScopeRequest));

        assertEquals("Create scope status", CREATED.getStatusCode(), response.getStatus());
        Assert.assertEquals("Create scope response", scopeName, response.readEntity(ScopeProperty.class).getScopeName());
        log.info("Create scope: {} successful ", scopeName);

        log.info("Test ScopeRelatedTests passed successfully!");
    }
}
