/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.qe;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.TimeWindow;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.watermark.WatermarkSerializer;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.rest.generated.api.JacksonJsonProvider;
import io.pravega.controller.server.rest.generated.model.StreamProperty;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.shared.NameUtils;
import io.pravega.shared.watermarks.Watermark;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import static javax.ws.rs.core.Response.Status.OK;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

@Slf4j
@RunWith(SystemTestRunner.class)
public class WatermarkSanityTest extends AbstractSystemTest {

    private static final String STREAM = "WMStream"+ RandomStringUtils.randomAlphanumeric(4);
    private static final String SCOPE = "testWMSanityScope";

    @Rule
    public Timeout globalTimeout = Timeout.seconds(10 * 60);

    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(5);
    private final StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();
    private Service controllerInstance;
    private URI controllerURI;
    private StreamManager streamManager;
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(5);

    /**
     * This is used to setup the various services required by the system test framework.
     *
     * @throws MarathonException    when error in setup
     */
    @Environment
    public static void initialize() throws MarathonException, ExecutionException {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = startPravegaControllerInstances(zkUri, 2);
        ensureSegmentStoreRunning(zkUri, controllerUri);
    }

    @Before
    public void setup() {
        controllerInstance = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = controllerInstance.getServiceDetails();
        final List<String> uris = ctlURIs.stream().filter(ISGRPC).map(URI::getAuthority).collect(Collectors.toList());

        controllerURI = URI.create("tcp://" + String.join(",", uris));
        streamManager = StreamManager.create(Utils.buildClientConfig(controllerURI));
        assertTrue("Creating Scope", streamManager.createScope(SCOPE));
        assertTrue("Creating stream", streamManager.createStream(SCOPE, STREAM, config));
    }

    @After
    public void tearDown() {
        streamManager.close();
    }

    // Test to verify markup stream is generated when writer sends noteTime to controller
    @Test
    public void verifyMarkStream() {
        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(clientConfig);
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(),
                connectionFactory.getInternalExecutor());

        // create one writer
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, clientConfig);
        JavaSerializer<Long> javaSerializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<Long> writer1 = clientFactory.createEventWriter(STREAM, javaSerializer,
                EventWriterConfig.builder().build());

        AtomicBoolean stopFlag = new AtomicBoolean(false);
        // write events
        writeEvents(writer1, stopFlag);

        List<URI> ctlURIs = controllerInstance.getServiceDetails();
        URI controllerRESTUri = ctlURIs.get(1);
        String restServerURI = "http://" + controllerRESTUri.getHost() + ":" + controllerRESTUri.getPort();
        log.info("REST Server URI: {}", restServerURI);

        org.glassfish.jersey.client.ClientConfig restClientConfig = new org.glassfish.jersey.client.ClientConfig();
        restClientConfig.register(JacksonJsonProvider.class);
        restClientConfig.property("sun.net.http.allowRestrictedHeaders", "true");
        Client client = ClientBuilder.newClient(restClientConfig);

        String marks_stream = NameUtils.getMarkStreamForStream(STREAM);
        String resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + SCOPE + "/streams/" + marks_stream).toString();
        Response response = client.target(resourceURl).request().get();
        assertEquals("Get marks_stream status", OK.getStatusCode(), response.getStatus());
        assertEquals("Get marks_stream response", marks_stream, response.readEntity(StreamProperty.class).getStreamName());
        log.info("Get marks_stream {} successful", marks_stream);
    }

    // Test multiple watermarks upon minimum controller instances
    @Test
    public void writeWMWithOneControllerInstance() throws Exception {
        // scale down one controller instance.
        Futures.getAndHandleExceptions(controllerInstance.scaleService(1), ExecutionException::new);

        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(clientConfig);
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(),
                connectionFactory.getInternalExecutor());

        // create 2 writers
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, clientConfig);
        JavaSerializer<Long> javaSerializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<Long> writer1 = clientFactory.createEventWriter(STREAM, javaSerializer,
                EventWriterConfig.builder().build());
        @Cleanup
        EventStreamWriter<Long> writer2 = clientFactory.createEventWriter(STREAM, javaSerializer,
                EventWriterConfig.builder().build());

        AtomicBoolean stopFlag = new AtomicBoolean(false);
        // write events
        writeEvents(writer1, stopFlag);
        writeEvents(writer2, stopFlag);

        // wait until mark stream is created
        AtomicBoolean markStreamCreated = new AtomicBoolean(false);
        Futures.loop(() -> !markStreamCreated.get(),
                () -> Futures.exceptionallyExpecting(controller.getCurrentSegments(SCOPE, NameUtils.getMarkStreamForStream(STREAM)),
                        e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, null)
                        .thenAccept(v -> markStreamCreated.set(v != null)), executorService);

        @Cleanup
        SynchronizerClientFactory syncClientFactory = SynchronizerClientFactory.withScope(SCOPE, clientConfig);
        String markStream = NameUtils.getMarkStreamForStream(STREAM);

        RevisionedStreamClient<Watermark> watermarkReader = syncClientFactory.createRevisionedStreamClient(markStream,
                new WatermarkSerializer(),
                SynchronizerConfig.builder().build());

        LinkedBlockingQueue<Watermark> watermarks = new LinkedBlockingQueue<>();
        fetchWatermarks(watermarkReader, watermarks, stopFlag);

        AssertExtensions.assertEventuallyEquals(true, () -> watermarks.size() >= 2, 100000);
        // wait until at least 2 more watermarks are emitted
        AssertExtensions.assertEventuallyEquals(true, () -> watermarks.size() >= 4, 100000);

        stopFlag.set(true);

        Watermark watermark0 = watermarks.take();
        Watermark watermark1 = watermarks.take();
        Watermark watermark2 = watermarks.take();
        Watermark watermark3 = watermarks.take();

        assertTrue(watermark0.getLowerTimeBound() <= watermark0.getUpperTimeBound());
        assertTrue(watermark1.getLowerTimeBound() <= watermark1.getUpperTimeBound());
        assertTrue(watermark2.getLowerTimeBound() <= watermark2.getUpperTimeBound());
        assertTrue(watermark3.getLowerTimeBound() <= watermark3.getUpperTimeBound());

        // verify that watermarks are increasing in time.
        assertTrue(watermark0.getLowerTimeBound() < watermark1.getLowerTimeBound());
        assertTrue(watermark1.getLowerTimeBound() < watermark2.getLowerTimeBound());
        assertTrue(watermark2.getLowerTimeBound() < watermark3.getLowerTimeBound());

    }

    //Test to verify the new TimeWindow is greater than the previous TimeWindow of watermark
    @Test
    public void verifyTimeWindow() throws Exception {
        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(clientConfig);
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(),
                connectionFactory.getInternalExecutor());

        // create 2 writers
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, clientConfig);
        JavaSerializer<Long> javaSerializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<Long> writer1 = clientFactory.createEventWriter(STREAM, javaSerializer,
                EventWriterConfig.builder().build());
        @Cleanup
        EventStreamWriter<Long> writer2 = clientFactory.createEventWriter(STREAM, javaSerializer,
                EventWriterConfig.builder().build());

        AtomicBoolean stopFlag = new AtomicBoolean(false);
        // write events
        writeEvents(writer1, stopFlag);
        writeEvents(writer2, stopFlag);

        // scale the stream several times so that we get complex positions
        Stream streamObj = Stream.of(SCOPE, STREAM);
        scale(controller, streamObj);

        // wait until mark stream is created
        AtomicBoolean markStreamCreated = new AtomicBoolean(false);
        Futures.loop(() -> !markStreamCreated.get(),
                () -> Futures.exceptionallyExpecting(controller.getCurrentSegments(SCOPE, NameUtils.getMarkStreamForStream(STREAM)),
                        e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, null)
                        .thenAccept(v -> markStreamCreated.set(v != null)), executorService);

        @Cleanup
        SynchronizerClientFactory syncClientFactory = SynchronizerClientFactory.withScope(SCOPE, clientConfig);
        String markStream = NameUtils.getMarkStreamForStream(STREAM);

        RevisionedStreamClient<Watermark> watermarkReader = syncClientFactory.createRevisionedStreamClient(markStream,
                new WatermarkSerializer(),
                SynchronizerConfig.builder().build());

        LinkedBlockingQueue<Watermark> watermarks = new LinkedBlockingQueue<>();
        fetchWatermarks(watermarkReader, watermarks, stopFlag);

        AssertExtensions.assertEventuallyEquals(true, () -> watermarks.size() >= 2, 100000);

        // wait until at least 2 more watermarks are emitted
        AssertExtensions.assertEventuallyEquals(true, () -> watermarks.size() >= 4, 100000);

        stopFlag.set(true);

        Watermark watermark0 = watermarks.take();
        Watermark watermark1 = watermarks.take();
        Watermark watermark2 = watermarks.take();
        Watermark watermark3 = watermarks.take();

        // verify that new TimeWindow is greater than the previous TimeWindow of watermark.
        assertTrue(watermark0.getLowerTimeBound() < watermark1.getLowerTimeBound());
        assertTrue(watermark1.getLowerTimeBound() < watermark2.getLowerTimeBound());
        assertTrue(watermark2.getLowerTimeBound() < watermark3.getLowerTimeBound());

        assertTrue(watermark0.getUpperTimeBound() < watermark1.getUpperTimeBound());
        assertTrue(watermark1.getUpperTimeBound() < watermark2.getUpperTimeBound());
        assertTrue(watermark2.getUpperTimeBound() < watermark3.getUpperTimeBound());


        // use watermark as lower and upper bounds.
        Map<Segment, Long> positionMap0 = watermark0.getStreamCut()
                .entrySet().stream()
                .collect(Collectors.toMap(x ->
                                new Segment(SCOPE, STREAM, x.getKey().getSegmentId()),
                        Map.Entry::getValue));

        StreamCut streamCutStart = new StreamCutImpl(streamObj, positionMap0);
        Map<Stream, StreamCut> start = Collections.singletonMap(streamObj, streamCutStart);

        Map<Segment, Long> positionMap2 = watermark2.getStreamCut()
                .entrySet().stream()
                .collect(Collectors.toMap(x ->
                                new Segment(SCOPE, STREAM, x.getKey().getSegmentId()),
                        Map.Entry::getValue));

        StreamCut streamCutEnd = new StreamCutImpl(streamObj, positionMap2);
        Map<Stream, StreamCut> end = Collections.singletonMap(streamObj, streamCutEnd);

        @Cleanup
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(SCOPE, controller, syncClientFactory, connectionFactory);
        String readerGroup = "rg";

        readerGroupManager.createReaderGroup(readerGroup, ReaderGroupConfig.builder().stream(streamObj)
                .startingStreamCuts(start)
                .endingStreamCuts(end).build());

        // create reader on the stream
        @Cleanup
        final EventStreamReader<Long> reader = clientFactory.createReader("myreader",
                readerGroup,
                javaSerializer,
                ReaderConfig.builder().build());

        // read events from the reader.
        // verify that events read belong to the bound
        EventRead<Long> event = reader.readNextEvent(10000L);
        TimeWindow currentTimeWindow = reader.getCurrentTimeWindow(streamObj);
        assertNotNull(currentTimeWindow);
        assertNotNull(currentTimeWindow.getLowerTimeBound());
        assertNotNull(currentTimeWindow.getUpperTimeBound());
        log.info("current time window = {}", currentTimeWindow);

        while (event.getEvent() != null) {
            Long time = event.getEvent();
            log.info("event read = {}", time);
            event.getPosition();
            assertTrue(time >= currentTimeWindow.getLowerTimeBound());
            event = reader.readNextEvent(10000L);
            if (event.isCheckpoint()) {
                event = reader.readNextEvent(10000L);
            }
        }
    }

    private void fetchWatermarks(RevisionedStreamClient<Watermark> watermarkReader, LinkedBlockingQueue<Watermark> watermarks, AtomicBoolean stop) throws Exception {
        AtomicReference<Revision> revision = new AtomicReference<>(watermarkReader.fetchOldestRevision());

        Futures.loop(() -> !stop.get(), () -> Futures.delayedTask(() -> {
            Iterator<Map.Entry<Revision, Watermark>> marks = watermarkReader.readFrom(revision.get());
            if (marks.hasNext()) {
                Map.Entry<Revision, Watermark> next = marks.next();
                log.info("watermark = {}", next.getValue());
                watermarks.add(next.getValue());
                revision.set(next.getKey());
            }
            return null;
        }, Duration.ofSeconds(10), executorService), executorService);
    }

    private void scale(Controller controller, Stream streamObj) throws InterruptedException {
        // perform several scales
        int numOfSegments = config.getScalingPolicy().getMinNumSegments();
        double delta = 1.0 / numOfSegments;
        for (long segmentNumber = 0; segmentNumber < numOfSegments - 1; segmentNumber++) {
            Thread.sleep(5000L);
            double rangeLow = segmentNumber * delta;
            double rangeHigh = (segmentNumber + 1) * delta;
            double rangeMid = (rangeHigh + rangeLow) / 2;

            Map<Double, Double> map = new HashMap<>();
            map.put(rangeLow, rangeMid);
            map.put(rangeMid, rangeHigh);
            controller.scaleStream(streamObj, Collections.singletonList(segmentNumber), map, executorService).getFuture().join();
        }
    }

    private void writeEvents(EventStreamWriter<Long> writer, AtomicBoolean stopFlag) {
        AtomicInteger count = new AtomicInteger(0);
        AtomicLong currentTime = new AtomicLong();
        Futures.loop(() -> !stopFlag.get(), () -> Futures.delayedFuture(() -> {
            currentTime.set(System.currentTimeMillis());
            return writer.writeEvent(count.toString(), currentTime.get())
                    .thenAccept(v -> {
                        if (count.incrementAndGet() % 10 == 0) {
                            writer.noteTime(currentTime.get());
                        }
                    });
        }, 1000L, executorService), executorService);
    }
}
