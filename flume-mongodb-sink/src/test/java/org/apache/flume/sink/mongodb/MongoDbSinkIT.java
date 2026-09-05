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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration tests that exercise {@link MongoDbSink} against MongoDB running
 * in a Testcontainers-managed Docker container. The tests are skipped
 * (rather than failed) when Docker is not available in the environment.
 */
public class MongoDbSinkIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:latest");

    private static MongoDBContainer mongoDBContainer;
    private static MongoClient mongoClient;

    @BeforeClass
    public static void startMongoContainer() {
        Assume.assumeTrue(
                "Docker is not available, skipping MongoDbSinkIT",
                DockerClientFactory.instance().isDockerAvailable());

        mongoDBContainer = new MongoDBContainer(MONGO_IMAGE);
        mongoDBContainer.start();
        mongoClient = MongoClients.create(mongoDBContainer.getConnectionString());
    }

    @AfterClass
    public static void stopMongoContainer() {
        if (mongoClient != null) {
            mongoClient.close();
        }
        if (mongoDBContainer != null) {
            mongoDBContainer.stop();
        }
    }

    private static Context baseContext(String database, String collection) {
        Context context = new Context();
        context.put(MongoDbSinkConstants.CONNECTION_URI, mongoDBContainer.getConnectionString());
        context.put(MongoDbSinkConstants.DATABASE_NAME, database);
        context.put(MongoDbSinkConstants.COLLECTION, collection);
        return context;
    }

    private static MongoDbSink createAndStartSink(Context context) {
        MongoDbSink sink = new MongoDbSink();
        Channel channel = new MemoryChannel();
        Configurables.configure(channel, new Context());
        sink.setChannel(channel);
        channel.start();
        Configurables.configure(sink, context);
        sink.start();
        return sink;
    }

    private static void putEvent(Channel channel, String json) {
        Transaction tx = channel.getTransaction();
        tx.begin();
        channel.put(EventBuilder.withBody(json.getBytes(StandardCharsets.UTF_8), new HashMap<>()));
        tx.commit();
        tx.close();
    }

    @Test
    public void testDuplicateKeyDoesNotFailBatchAndIsCountedSeparately() throws EventDeliveryException {
        String database = "testDb1";
        String collectionName = "events";
        Context context = baseContext(database, collectionName);
        MongoDbSink sink = createAndStartSink(context);
        try {
            MongoCollection<Document> collection =
                    mongoClient.getDatabase(database).getCollection(collectionName);
            collection.createIndex(Indexes.ascending("uid"), new IndexOptions().unique(true));

            Channel channel = sink.getChannel();
            // Two distinct events plus one that duplicates the first's unique key.
            putEvent(channel, "{\"uid\":1,\"value\":\"a\"}");
            putEvent(channel, "{\"uid\":2,\"value\":\"b\"}");
            putEvent(channel, "{\"uid\":1,\"value\":\"c\"}");

            Sink.Status status = sink.process();

            assertEquals(Sink.Status.READY, status);
            assertEquals(2, collection.countDocuments());
            assertEquals(1, sink.getDuplicateEventCount());
        } finally {
            sink.stop();
        }
    }

    @Test
    public void testNoDuplicatesLeavesDuplicateCountAtZero() throws EventDeliveryException {
        String database = "testDb2";
        String collectionName = "events";
        Context context = baseContext(database, collectionName);
        MongoDbSink sink = createAndStartSink(context);
        try {
            MongoCollection<Document> collection =
                    mongoClient.getDatabase(database).getCollection(collectionName);
            collection.createIndex(Indexes.ascending("uid"), new IndexOptions().unique(true));

            Channel channel = sink.getChannel();
            putEvent(channel, "{\"uid\":1,\"value\":\"a\"}");
            putEvent(channel, "{\"uid\":2,\"value\":\"b\"}");

            Sink.Status status = sink.process();

            assertEquals(Sink.Status.READY, status);
            assertEquals(2, collection.countDocuments());
            assertEquals(0, sink.getDuplicateEventCount());
        } finally {
            sink.stop();
        }
    }
}
