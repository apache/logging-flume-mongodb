/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.sink.mongodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.bson.Document;
import org.junit.Test;

public class TestMongoDbSink {

    /**
     * In-memory {@link MongoDbWriter} fake used to capture what the sink
     * would have written, without needing a live MongoDB connection.
     */
    private static final class FakeMongoDbWriter implements MongoDbWriter {
        private final Map<String, List<Document>> written = new LinkedHashMap<>();
        private long duplicateCountToReport = 0;

        @Override
        public MongoDbWriteResult write(String collectionName, List<Document> documents) {
            written.computeIfAbsent(collectionName, k -> new ArrayList<>()).addAll(documents);
            long duplicates = Math.min(duplicateCountToReport, documents.size());
            duplicateCountToReport -= duplicates;
            return new MongoDbWriteResult(documents.size() - duplicates, duplicates);
        }

        @Override
        public void close() {}
    }

    private static Context baseContext() {
        Context context = new Context();
        context.put(MongoDbSinkConstants.CONNECTION_URI, "mongodb://localhost:27017");
        context.put(MongoDbSinkConstants.DATABASE_NAME, "testDb");
        context.put(MongoDbSinkConstants.COLLECTION, "defaultCollection");
        return context;
    }

    private static void setInternalState(Object target, String fieldName, Object value) {
        try {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Unable to set field " + fieldName, e);
        }
    }

    private static Object getInternalState(Object target, String fieldName) {
        try {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(target);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Unable to read field " + fieldName, e);
        }
    }

    private static MongoDbSink createSink(Context context, MongoDbWriter writer) {
        MongoDbSink sink = new MongoDbSink();
        Channel channel = new MemoryChannel();
        Configurables.configure(channel, new Context());
        sink.setChannel(channel);
        channel.start();
        Configurables.configure(sink, context);
        setInternalState(sink, "writer", writer);
        setInternalState(sink, "counter", new MongoDbSinkCounter("test"));
        return sink;
    }

    private static void putEvent(Channel channel, byte[] body, Map<String, String> headers) {
        Transaction tx = channel.getTransaction();
        tx.begin();
        channel.put(EventBuilder.withBody(body, headers));
        tx.commit();
        tx.close();
    }

    @Test(expected = ConfigurationException.class)
    public void testConfigureMissingUri() {
        Context context = new Context();
        context.put(MongoDbSinkConstants.DATABASE_NAME, "testDb");
        context.put(MongoDbSinkConstants.COLLECTION, "col");
        new MongoDbSink().configure(context);
    }

    @Test(expected = ConfigurationException.class)
    public void testConfigureMissingDatabase() {
        Context context = new Context();
        context.put(MongoDbSinkConstants.CONNECTION_URI, "mongodb://localhost:27017");
        context.put(MongoDbSinkConstants.COLLECTION, "col");
        new MongoDbSink().configure(context);
    }

    @Test(expected = ConfigurationException.class)
    public void testConfigureMissingCollection() {
        Context context = new Context();
        context.put(MongoDbSinkConstants.CONNECTION_URI, "mongodb://localhost:27017");
        context.put(MongoDbSinkConstants.DATABASE_NAME, "testDb");
        new MongoDbSink().configure(context);
    }

    @Test
    public void testDuplicateEventsDoNotFailBatchAndAreCountedSeparately() throws EventDeliveryException {
        FakeMongoDbWriter writer = new FakeMongoDbWriter();
        writer.duplicateCountToReport = 1;
        Context context = baseContext();
        MongoDbSink sink = createSink(context, writer);
        Channel channel = sink.getChannel();

        putEvent(channel, "{\"foo\":\"1\"}".getBytes(StandardCharsets.UTF_8), new HashMap<String, String>());
        putEvent(channel, "{\"foo\":\"2\"}".getBytes(StandardCharsets.UTF_8), new HashMap<String, String>());

        Sink.Status status = sink.process();

        assertEquals(Sink.Status.READY, status);
        assertEquals(1, sink.getDuplicateEventCount());
    }

    @Test
    public void testWritesToDefaultCollectionWhenNoHeaderConfigured() throws EventDeliveryException {
        FakeMongoDbWriter writer = new FakeMongoDbWriter();
        Context context = baseContext();
        MongoDbSink sink = createSink(context, writer);
        Channel channel = sink.getChannel();

        putEvent(channel, "{\"foo\":\"bar\"}".getBytes(StandardCharsets.UTF_8), new HashMap<String, String>());

        Sink.Status status = sink.process();
        assertEquals(Sink.Status.READY, status);

        List<Document> docs = writer.written.get("defaultCollection");
        assertEquals(1, docs.size());
        assertEquals("bar", docs.get(0).getString("foo"));
    }

    @Test
    public void testHeaderMapsToPredefinedCollection() throws EventDeliveryException {
        FakeMongoDbWriter writer = new FakeMongoDbWriter();
        Context context = baseContext();
        context.put(MongoDbSinkConstants.COLLECTION_HEADER, "type");
        context.put(MongoDbSinkConstants.COLLECTION_MAP_PREFIX + "typeA", "collectionA");

        MongoDbSink sink = createSink(context, writer);
        Channel channel = sink.getChannel();

        Map<String, String> headers = new HashMap<>();
        headers.put("type", "typeA");
        putEvent(channel, "{\"foo\":\"bar\"}".getBytes(StandardCharsets.UTF_8), headers);

        Sink.Status status = sink.process();
        assertEquals(Sink.Status.READY, status);

        assertTrue(writer.written.containsKey("collectionA"));
        assertFalse(writer.written.containsKey("defaultCollection"));
        assertEquals(1, writer.written.get("collectionA").size());
    }

    @Test
    public void testUnmappedHeaderFallsBackToDefaultCollection() throws EventDeliveryException {
        FakeMongoDbWriter writer = new FakeMongoDbWriter();
        Context context = baseContext();
        context.put(MongoDbSinkConstants.COLLECTION_HEADER, "type");
        context.put(MongoDbSinkConstants.COLLECTION_MAP_PREFIX + "typeA", "collectionA");

        MongoDbSink sink = createSink(context, writer);
        Channel channel = sink.getChannel();

        Map<String, String> headers = new HashMap<>();
        headers.put("type", "typeUnknown");
        putEvent(channel, "{\"foo\":\"bar\"}".getBytes(StandardCharsets.UTF_8), headers);

        Sink.Status status = sink.process();
        assertEquals(Sink.Status.READY, status);

        assertTrue(writer.written.containsKey("defaultCollection"));
        assertFalse(writer.written.containsKey("collectionA"));
    }

    @Test
    public void testUnmappedHeaderFailsWhenFallbackDisabled() throws EventDeliveryException {
        FakeMongoDbWriter writer = new FakeMongoDbWriter();
        Context context = baseContext();
        context.put(MongoDbSinkConstants.COLLECTION_HEADER, "type");
        context.put(MongoDbSinkConstants.COLLECTION_MAP_PREFIX + "typeA", "collectionA");
        context.put(MongoDbSinkConstants.COLLECTION_MAP_FALLBACK, "false");

        MongoDbSink sink = createSink(context, writer);
        Channel channel = sink.getChannel();

        Map<String, String> headers = new HashMap<>();
        headers.put("type", "typeUnknown");
        putEvent(channel, "{\"foo\":\"bar\"}".getBytes(StandardCharsets.UTF_8), headers);

        try {
            sink.process();
            fail("Expected EventDeliveryException");
        } catch (EventDeliveryException expected) {
            assertTrue(expected.getCause().getMessage().contains("No collection mapping"));
        }
        assertTrue(writer.written.isEmpty());
    }

    @Test
    public void testNonJsonBodyIsWrappedInMessageField() throws EventDeliveryException {
        FakeMongoDbWriter writer = new FakeMongoDbWriter();
        Context context = baseContext();
        MongoDbSink sink = createSink(context, writer);
        Channel channel = sink.getChannel();

        putEvent(channel, "plain text event".getBytes(StandardCharsets.UTF_8), new HashMap<String, String>());

        sink.process();

        List<Document> docs = writer.written.get("defaultCollection");
        assertEquals("plain text event", docs.get(0).getString(MongoDbSinkConstants.BODY_FIELD));
    }

    @Test
    public void testIncludeHeadersAddsHeadersField() throws EventDeliveryException {
        FakeMongoDbWriter writer = new FakeMongoDbWriter();
        Context context = baseContext();
        context.put(MongoDbSinkConstants.INCLUDE_HEADERS, "true");
        MongoDbSink sink = createSink(context, writer);
        Channel channel = sink.getChannel();

        Map<String, String> headers = new HashMap<>();
        headers.put("source", "app1");
        putEvent(channel, "{\"foo\":\"bar\"}".getBytes(StandardCharsets.UTF_8), headers);

        sink.process();

        Document doc = writer.written.get("defaultCollection").get(0);
        Document headerDoc = (Document) doc.get(MongoDbSinkConstants.HEADERS_FIELD);
        assertEquals("app1", headerDoc.getString("source"));
    }

    @Test
    public void testBackoffOnEmptyChannel() throws EventDeliveryException {
        FakeMongoDbWriter writer = new FakeMongoDbWriter();
        Context context = baseContext();
        MongoDbSink sink = createSink(context, writer);

        Sink.Status status = sink.process();
        assertEquals(Sink.Status.BACKOFF, status);
        assertTrue(writer.written.isEmpty());
    }

    @Test
    public void testCollectionMapParsedFromSubProperties() {
        Context context = baseContext();
        context.put(MongoDbSinkConstants.COLLECTION_HEADER, "type");
        context.put(MongoDbSinkConstants.COLLECTION_MAP_PREFIX + "typeA", "collectionA");
        context.put(MongoDbSinkConstants.COLLECTION_MAP_PREFIX + "typeB", "collectionB");

        MongoDbSink sink = new MongoDbSink();
        sink.configure(context);

        @SuppressWarnings("unchecked")
        Map<String, String> collectionMap = (Map<String, String>) getInternalState(sink, "collectionMap");
        assertEquals("collectionA", collectionMap.get("typeA"));
        assertEquals("collectionB", collectionMap.get("typeB"));
    }
}
