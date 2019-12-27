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
import io.pravega.controller.server.rest.generated.model.ScopeProperty;
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

import org.apache.commons.lang.math.RandomUtils;
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
public class ScopeTest extends AbstractSystemTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(5 * 60);

    private final Client client;
    private WebTarget webTarget;
    private String resourceURl;
    private static String restServerURI;
    private static Service conService;
    private static URI controllerRESTUri;

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

    @Before
    public void setup() {
        conService = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = conService.getServiceDetails();
        controllerRESTUri = ctlURIs.get(1);
        restServerURI = "http://" + controllerRESTUri.getHost() + ":" + controllerRESTUri.getPort();
        log.info("REST Server URI: {}", restServerURI);
    }

    // Test _system scope is present by default
    @Test
    public void defaultScopePresent() {
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + "_system").toString();
        Response response = client.target(resourceURl).request().get();
        assertEquals("Get scope status", OK.getStatusCode(), response.getStatus());
        assertEquals("Get scope _system response", "_system", response.readEntity(ScopeProperty.class).getScopeName());
        log.info("_system scope is present");
    }

    // Test delete _system scope
    @Test
    public void deleteSystemScope(){
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + "_system").toString();
        Response response = client.target(resourceURl).request().delete();
        assertEquals("Delete scope _system status", PRECONDITION_FAILED.getStatusCode(), response.getStatus());
        log.info("Delete scope _system not possible, which is expected");
    }

    // Test create scope having alphanumeric characters
    @Test
    public void alphaNumericScope(){
        // POST http://controllerURI:Port/v1/scopes
        CreateScopeRequest createScopeRequest = new CreateScopeRequest();
        String scopeName = RandomStringUtils.randomAlphanumeric(5)+"12";

        // TEST CreateScope
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes").toString();
        webTarget = client.target(resourceURl);

        createScopeRequest.setScopeName(scopeName);
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        Response response = builder.post(Entity.json(createScopeRequest));

        assertEquals("Create scope status", CREATED.getStatusCode(), response.getStatus());
        Assert.assertEquals("Create scope response", scopeName, response.readEntity(ScopeProperty.class).getScopeName());
        log.info("Create scope: {} successful ", scopeName);
    }

    // Test create scope containing number
    @Test
    public void onlyNumberScope(){
        int random_int = RandomUtils.nextInt();

        String scopeName = String.valueOf(random_int);
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes").toString();
        webTarget = client.target(resourceURl);

        CreateScopeRequest createScopeRequest = new CreateScopeRequest();
        createScopeRequest.setScopeName(scopeName);
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        Response response = builder.post(Entity.json(createScopeRequest));

        assertEquals("Create scope status", CREATED.getStatusCode(), response.getStatus());
        Assert.assertEquals("Create scope response", scopeName, response.readEntity(ScopeProperty.class).getScopeName());
        log.info("Create scope: {} successful ", scopeName);
    }

    // Test create scope containing capital case letters
    @Test
    public void capitalCaseScope(){
        CreateScopeRequest createScopeRequest = new CreateScopeRequest();
        String scopeName = RandomStringUtils.randomAlphanumeric(6).toUpperCase();

        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes").toString();
        webTarget = client.target(resourceURl);

        createScopeRequest.setScopeName(scopeName);
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        Response response = builder.post(Entity.json(createScopeRequest));

        assertEquals("Create scope status", CREATED.getStatusCode(), response.getStatus());
        Assert.assertEquals("Create scope response", scopeName, response.readEntity(ScopeProperty.class).getScopeName());
        log.info("Create scope: {} successful ", scopeName);
    }

    // Test create scope containing small case letters
    @Test
    public void smallCaseScope(){
        CreateScopeRequest createScopeRequest = new CreateScopeRequest();
        String scopeName = RandomStringUtils.randomAlphanumeric(5).toLowerCase();

        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes").toString();
        webTarget = client.target(resourceURl);

        createScopeRequest.setScopeName(scopeName);
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        Response response = builder.post(Entity.json(createScopeRequest));

        assertEquals("Create scope status", CREATED.getStatusCode(), response.getStatus());
        Assert.assertEquals("Create scope response", scopeName, response.readEntity(ScopeProperty.class).getScopeName());
        log.info("Create scope: {} successful ", scopeName);
    }

    // Test create scope containing special characters
    @Test
    public void specialCharScope(){
        CreateScopeRequest createScopeRequest = new CreateScopeRequest();
        String scopeName = "i_am_underScore";

        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes").toString();
        webTarget = client.target(resourceURl);

        createScopeRequest.setScopeName(scopeName);
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        Response response = builder.post(Entity.json(createScopeRequest));

        assertEquals("Create scope status", BAD_REQUEST.getStatusCode(), response.getStatus());
        log.info("Create scope: {} having special character not possible, which is expected one", scopeName);
    }

    // Test create scope with an already existing scope name
    @Test
    public void duplicateScope(){
        CreateScopeRequest createScopeRequest = new CreateScopeRequest();
        String scopeName = RandomStringUtils.randomAlphanumeric(6).toLowerCase();

        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes").toString();
        webTarget = client.target(resourceURl);

        createScopeRequest.setScopeName(scopeName);
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        Response response = builder.post(Entity.json(createScopeRequest));

        assertEquals("Create scope status", CREATED.getStatusCode(), response.getStatus());
        Assert.assertEquals("Create scope response", scopeName, response.readEntity(ScopeProperty.class).getScopeName());

        createScopeRequest.setScopeName(scopeName);
        builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        response = builder.post(Entity.json(createScopeRequest));

        assertEquals("Create scope status", CONFLICT.getStatusCode(), response.getStatus());
        log.info("Not able to create duplicate scope, which is expected one");
    }

    // Test delete scope with active stream
    @Test
    public void deleteScopeWithActiveStream(){
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + "_system").toString();
        Response response = client.target(resourceURl).request().delete();
        assertEquals("Delete scope status", PRECONDITION_FAILED.getStatusCode(), response.getStatus());
        log.info("Delete scope with active stream not possible, which is expected");
    }

    // Test create multiple scopes
    @Test
    public void createMultipleScopes(){
        String scopeName = RandomStringUtils.randomAlphanumeric(6);
        createScopes(scopeName);
        for(int i=1; i<=3; i++){
            String sName = scopeName+i;
            resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + sName).toString();
            Response response = client.target(resourceURl).request().get();
            assertEquals("Get scope status for "+sName+" ", OK.getStatusCode(), response.getStatus());
            assertEquals("Get scope response for "+sName+" ", sName, response.readEntity(ScopeProperty.class).getScopeName());
        }

        log.info("Create multiple scopes successful");
    }

    // Test delete multiple scopes
    @Test
    public void deleteMultipleScopes(){
        String scopeName = RandomStringUtils.randomAlphanumeric(6);
        createScopes(scopeName);

        for(int i=1; i<=3; i++){
            String sName = scopeName+i;
            resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + sName).toString();
            Response response = client.target(resourceURl).request().delete();
            assertEquals("Delete scope status for "+sName+" ", NO_CONTENT.getStatusCode(), response.getStatus());
        }

        log.info("Delete multiple scopes successful");
    }

    private void createScopes(String scopeName){
        CreateScopeRequest createScopeRequest = new CreateScopeRequest();
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes").toString();
        webTarget = client.target(resourceURl);

        for(int i=1; i<=3; i++){
            String sName = scopeName+i;
            createScopeRequest.setScopeName(sName);
            Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
            Response response = builder.post(Entity.json(createScopeRequest));
            assertEquals("Create scope status for "+sName+" ", CREATED.getStatusCode(), response.getStatus());
        }
    }


}
