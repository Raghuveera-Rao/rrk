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

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class ComponentScaleTest extends AbstractComponentScaleTest {

    private static final String STREAM_NAME = "testComponentScaleStream";
    private static final int NUM_WRITERS = 5;
    private static final int NUM_READERS = 5;

    //The execution time for @Before + @After + @Test methods should be less than 15 mins. Else the test will timeout.
    @Rule
    public Timeout globalTimeout = Timeout.seconds(15 * 60);
    private final String scope = "testComponentScaleScope" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private final String readerGroupName = "testComponentScaleReaderGroup" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(NUM_READERS);
    StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();
    private ClientFactoryImpl clientFactory;
    private ReaderGroupManager readerGroupManager;
    private StreamManager streamManager;

    @Environment
    public static void initialize() throws MarathonException, ExecutionException {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = startPravegaControllerInstances(zkUri, 2);
        startPravegaSegmentStoreInstances(zkUri, controllerUri, 3);
    }

    @Before
    public void setup() {

        // Get zk details to verify if controller, SSS are running
        zookeeperInstance = Utils.createZookeeperService();
        List<URI> zkUris = zookeeperInstance.getServiceDetails();
        log.debug("Zookeeper service details: {}", zkUris);
        //get the zk ip details and pass it to  host, controller
        URI zkUri = zkUris.get(0);

        // Verify bookkeeper is running
        bookkeeperInstance = Utils.createBookkeeperService(zkUri);
        assertTrue(bookkeeperInstance.isRunning());
        log.info("Bookkeeper service instance details: {}", bookkeeperInstance.getServiceDetails());

        // Verify controller is running.
        controllerInstance = Utils.createPravegaControllerService(zkUri);
        assertTrue(controllerInstance.isRunning());
        List<URI> conURIs = controllerInstance.getServiceDetails();
        log.info("Pravega Controller service instance details: {}", conURIs);

        // Fetch all the RPC endpoints and construct the client URIs.
        final List<String> uris = conURIs.stream().filter(ISGRPC).map(URI::getAuthority).collect(Collectors.toList());

        controllerURIDirect = URI.create("tcp://" + String.join(",", uris));
        log.info("Controller Service direct URI: {}", controllerURIDirect);

        // Verify segment store is running.
        segmentStoreInstance = Utils.createPravegaSegmentStoreService(zkUri, controllerURIDirect);
        assertTrue(segmentStoreInstance.isRunning());
        log.info("Pravega Segmentstore service instance details: {}", segmentStoreInstance.getServiceDetails());

        executorService = ExecutorServiceHelpers.newScheduledThreadPool(NUM_READERS + NUM_WRITERS + 1, "MultiReaderWriterWithFailOverTest-main");

        controllerExecutorService = ExecutorServiceHelpers.newScheduledThreadPool(2,
                "MultiReaderWriterWithFailoverTest-controller");

        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURIDirect);
        //get Controller Uri
        controller = new ControllerImpl(ControllerImplConfig.builder()
                .clientConfig(clientConfig)
                .maxBackoffMillis(5000).build(),
                controllerExecutorService);

        testState = new TestState(false);

        //read and write count variables
        streamManager = new StreamManagerImpl(clientConfig);
        createScopeAndStream(scope, STREAM_NAME, config, streamManager);
        log.info("Scope passed to client factory {}", scope);
        clientFactory = new ClientFactoryImpl(scope, controller, new ConnectionFactoryImpl(clientConfig));
        readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
    }

    @After
    public void tearDown() throws ExecutionException {
        testState.stopReadFlag.set(true);
        testState.stopWriteFlag.set(true);
        //interrupt writers and readers threads if they are still running.
        testState.cancelAllPendingWork();
        streamManager.close();
        clientFactory.close(); //close the clientFactory/connectionFactory.
        readerGroupManager.close();
        ExecutorServiceHelpers.shutdown(executorService, controllerExecutorService);
        //scale the controller and segmentStore back to 1 instance.
        Futures.getAndHandleExceptions(controllerInstance.scaleService(2), ExecutionException::new);
        Futures.getAndHandleExceptions(segmentStoreInstance.scaleService(3), ExecutionException::new);
        Futures.getAndHandleExceptions(bookkeeperInstance.scaleService(4), ExecutionException::new);
        Futures.getAndHandleExceptions(zookeeperInstance.scaleService(1), ExecutionException::new);
    }

    @Test
    public void scaleUpControllerTest() throws Exception {
        createWriters(clientFactory, NUM_WRITERS, scope, STREAM_NAME);
        createReaders(clientFactory, readerGroupName, scope, readerGroupManager, STREAM_NAME, NUM_READERS);

        Map<String, Integer> scaleMap = new HashMap<>();
        scaleMap.put("controller",5);
        //run the scale test
        scaleComponentTest(scaleMap);

        scaleMap.replace("controller",10);
        //run the scale test
        scaleComponentTest(scaleMap);

        stopWriters();
        stopReaders();
        validateResults();
        cleanUp(scope, STREAM_NAME, readerGroupManager, readerGroupName); //cleanup if validation is successful.
        testState.checkForAnomalies();
        log.info("Test scaleUpControllerTest succeeded");
    }

    @Test
    public void scaleUpBookieTest() throws Exception {
        createWriters(clientFactory, NUM_WRITERS, scope, STREAM_NAME);
        createReaders(clientFactory, readerGroupName, scope, readerGroupManager, STREAM_NAME, NUM_READERS);

        Map<String, Integer> scaleMap = new HashMap<>();
        scaleMap.put("bookkeeper",5);
        //run the scale test
        scaleComponentTest(scaleMap);

        scaleMap.replace("bookkeeper",10);
        //run the scale test
        scaleComponentTest(scaleMap);

        stopWriters();
        stopReaders();
        validateResults();
        cleanUp(scope, STREAM_NAME, readerGroupManager, readerGroupName); //cleanup if validation is successful.
        testState.checkForAnomalies();
        log.info("Test scaleUpBookieTest succeeded");
    }

    @Test
    public void scaleUpSegmentStoreTest() throws Exception {
        createWriters(clientFactory, NUM_WRITERS, scope, STREAM_NAME);
        createReaders(clientFactory, readerGroupName, scope, readerGroupManager, STREAM_NAME, NUM_READERS);

        Map<String, Integer> scaleMap = new HashMap<>();
        scaleMap.put("segmentstore",5);
        //run the scale test
        scaleComponentTest(scaleMap);

        scaleMap.replace("segmentstore",10);
        //run the scale test
        scaleComponentTest(scaleMap);

        stopWriters();
        stopReaders();
        validateResults();
        cleanUp(scope, STREAM_NAME, readerGroupManager, readerGroupName); //cleanup if validation is successful.
        testState.checkForAnomalies();
        log.info("Test scaleUpSegmentStoreTest succeeded");
    }

    //@Test
    public void scaleUpZookeeperTest() throws Exception {
        createWriters(clientFactory, NUM_WRITERS, scope, STREAM_NAME);
        createReaders(clientFactory, readerGroupName, scope, readerGroupManager, STREAM_NAME, NUM_READERS);

        Map<String, Integer> scaleMap = new HashMap<>();
        scaleMap.put("zookeeper",3);
        //run the scale test
        scaleComponentTest(scaleMap);

        scaleMap.replace("zookeeper",5);
        //run the scale test
        scaleComponentTest(scaleMap);

        scaleMap.replace("zookeeper",7);
        //run the scale test
        scaleComponentTest(scaleMap);

        stopWriters();
        stopReaders();
        validateResults();
        cleanUp(scope, STREAM_NAME, readerGroupManager, readerGroupName); //cleanup if validation is successful.
        testState.checkForAnomalies();
        log.info("Test scaleUpZookeeperTest succeeded");
    }

    @Test
    public void scaleUpBookieContSSTest() throws Exception {
        createWriters(clientFactory, NUM_WRITERS, scope, STREAM_NAME);
        createReaders(clientFactory, readerGroupName, scope, readerGroupManager, STREAM_NAME, NUM_READERS);

        Map<String, Integer> scaleMap = new HashMap<>();
        scaleMap.put("bookkeeper",5);
        scaleMap.put("controller",4);
        scaleMap.put("segmentstore",5);
        //run the scale test
        scaleComponentTest(scaleMap);

        scaleMap.replace("bookkeeper",10);
        scaleMap.replace("controller",8);
        scaleMap.replace("segmentstore",10);
        //run the scale test
        scaleComponentTest(scaleMap);

        stopWriters();
        stopReaders();
        validateResults();
        cleanUp(scope, STREAM_NAME, readerGroupManager, readerGroupName); //cleanup if validation is successful.
        testState.checkForAnomalies();
        log.info("Test scaleUpBookieContSSTest succeeded");
    }

    //@Test
    public void scaleUpAllServicesTest() throws Exception {
        createWriters(clientFactory, NUM_WRITERS, scope, STREAM_NAME);
        createReaders(clientFactory, readerGroupName, scope, readerGroupManager, STREAM_NAME, NUM_READERS);

        Map<String, Integer> scaleMap = new HashMap<>();
        scaleMap.put("bookkeeper",5);
        scaleMap.put("controller",4);
        scaleMap.put("segmentstore",5);
        scaleMap.put("zookeeper",3);
        //run the scale test
        scaleComponentTest(scaleMap);

        scaleMap.replace("bookkeeper",10);
        scaleMap.replace("controller",6);
        scaleMap.replace("segmentstore",10);
        scaleMap.replace("zookeeper",5);
        //run the scale test
        scaleComponentTest(scaleMap);

        stopWriters();
        stopReaders();
        validateResults();
        cleanUp(scope, STREAM_NAME, readerGroupManager, readerGroupName); //cleanup if validation is successful.
        testState.checkForAnomalies();
        log.info("Test scaleUpAllServicesTest succeeded");
    }

    @Test
    public void scaleDownControllerTest() throws Exception {
        createWriters(clientFactory, NUM_WRITERS, scope, STREAM_NAME);
        createReaders(clientFactory, readerGroupName, scope, readerGroupManager, STREAM_NAME, NUM_READERS);

        Map<String, Integer> scaleMap = new HashMap<>();
        scaleMap.put("controller",10);
        //run the scale test
        scaleComponentTest(scaleMap);

        scaleMap.replace("controller",5);
        //run the scale test
        scaleComponentTest(scaleMap);

        scaleMap.replace("controller",2);
        //run the scale test
        scaleComponentTest(scaleMap);

        stopWriters();
        stopReaders();
        validateResults();
        cleanUp(scope, STREAM_NAME, readerGroupManager, readerGroupName); //cleanup if validation is successful.
        testState.checkForAnomalies();
        log.info("Test scaleDownControllerTest succeeded");
    }

    @Test
    public void scaleDownBookieTest() throws Exception {
        createWriters(clientFactory, NUM_WRITERS, scope, STREAM_NAME);
        createReaders(clientFactory, readerGroupName, scope, readerGroupManager, STREAM_NAME, NUM_READERS);

        Map<String, Integer> scaleMap = new HashMap<>();
        scaleMap.put("bookkeeper",10);
        //run the scale test
        scaleComponentTest(scaleMap);

        scaleMap.replace("bookkeeper",5);
        //run the scale test
        scaleComponentTest(scaleMap);

        scaleMap.replace("bookkeeper",4);
        //run the scale test
        scaleComponentTest(scaleMap);

        stopWriters();
        stopReaders();
        validateResults();
        cleanUp(scope, STREAM_NAME, readerGroupManager, readerGroupName); //cleanup if validation is successful.
        testState.checkForAnomalies();
        log.info("Test scaleDownBookieTest succeeded");
    }

    @Test
    public void scaleDownSegmentStoreTest() throws Exception {
        createWriters(clientFactory, NUM_WRITERS, scope, STREAM_NAME);
        createReaders(clientFactory, readerGroupName, scope, readerGroupManager, STREAM_NAME, NUM_READERS);

        Map<String, Integer> scaleMap = new HashMap<>();
        scaleMap.put("segmentstore",10);
        //run the scale test
        scaleComponentTest(scaleMap);

        scaleMap.replace("segmentstore",5);
        //run the scale test
        scaleComponentTest(scaleMap);

        scaleMap.replace("segmentstore",3);
        //run the scale test
        scaleComponentTest(scaleMap);

        stopWriters();
        stopReaders();
        validateResults();
        cleanUp(scope, STREAM_NAME, readerGroupManager, readerGroupName); //cleanup if validation is successful.
        testState.checkForAnomalies();
        log.info("Test scaleDownSegmentStoreTest succeeded");
    }

    //@Test
    public void scaleDownZookeeperTest() throws Exception {
        createWriters(clientFactory, NUM_WRITERS, scope, STREAM_NAME);
        createReaders(clientFactory, readerGroupName, scope, readerGroupManager, STREAM_NAME, NUM_READERS);

        Map<String, Integer> scaleMap = new HashMap<>();
        scaleMap.put("zookeeper",7);
        //run the scale test
        scaleComponentTest(scaleMap);

        scaleMap.replace("zookeeper",5);
        //run the scale test
        scaleComponentTest(scaleMap);

        scaleMap.replace("zookeeper",3);
        //run the scale test
        scaleComponentTest(scaleMap);

        stopWriters();
        stopReaders();
        validateResults();
        cleanUp(scope, STREAM_NAME, readerGroupManager, readerGroupName); //cleanup if validation is successful.
        testState.checkForAnomalies();
        log.info("Test scaleDownZookeeperTest succeeded");
    }

    @Test
    public void scaleDownBookieContSSTest() throws Exception {
        createWriters(clientFactory, NUM_WRITERS, scope, STREAM_NAME);
        createReaders(clientFactory, readerGroupName, scope, readerGroupManager, STREAM_NAME, NUM_READERS);

        Map<String, Integer> scaleMap = new HashMap<>();
        scaleMap.put("bookkeeper",10);
        scaleMap.put("controller",8);
        scaleMap.put("segmentstore",10);
        //run the scale test
        scaleComponentTest(scaleMap);

        scaleMap.replace("bookkeeper",5);
        scaleMap.replace("controller",4);
        scaleMap.replace("segmentstore",5);
        //run the scale test
        scaleComponentTest(scaleMap);

        scaleMap.replace("bookkeeper",4);
        scaleMap.replace("controller",2);
        scaleMap.replace("segmentstore",3);
        //run the scale test
        scaleComponentTest(scaleMap);

        stopWriters();
        stopReaders();
        validateResults();
        cleanUp(scope, STREAM_NAME, readerGroupManager, readerGroupName); //cleanup if validation is successful.
        testState.checkForAnomalies();
        log.info("Test scaleDownBookieContSSTest succeeded");
    }

    //@Test
    public void scaleDownAllServicesTest() throws Exception {
        createWriters(clientFactory, NUM_WRITERS, scope, STREAM_NAME);
        createReaders(clientFactory, readerGroupName, scope, readerGroupManager, STREAM_NAME, NUM_READERS);

        Map<String, Integer> scaleMap = new HashMap<>();
        scaleMap.put("bookkeeper",10);
        scaleMap.put("controller",6);
        scaleMap.put("segmentstore",10);
        scaleMap.put("zookeeper",5);
        //run the scale test
        scaleComponentTest(scaleMap);

        scaleMap.replace("bookkeeper",5);
        scaleMap.replace("controller",4);
        scaleMap.replace("segmentstore",5);
        scaleMap.replace("zookeeper",3);
        //run the scale test
        scaleComponentTest(scaleMap);

        scaleMap.replace("bookkeeper",4);
        scaleMap.replace("controller",2);
        scaleMap.replace("segmentstore",3);
        scaleMap.replace("zookeeper",1);
        //run the scale test
        scaleComponentTest(scaleMap);

        stopWriters();
        stopReaders();
        validateResults();
        cleanUp(scope, STREAM_NAME, readerGroupManager, readerGroupName); //cleanup if validation is successful.
        testState.checkForAnomalies();
        log.info("Test scaleDownAllServicesTest succeeded");
    }

}
