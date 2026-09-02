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

import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.sink.AbstractSink;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

/**
 * A Flume Sink that writes events to MongoDB.
 * <p/>
 * Each batch of events read from the channel is converted into MongoDB
 * {@link Document}s and inserted into a collection. The target collection
 * for a given event may be determined dynamically by reading a Flume event
 * header and mapping its value onto one of a set of predefined collection
 * names, allowing a single sink to fan out events to different collections
 * based on the header value.
 * <p/>
 * Mandatory properties are:
 * mongodb.uri -- the MongoDB connection string
 * mongodb.database -- the database to write to
 * mongodb.collection -- the default/fallback collection to write to
 * <p/>
 * Optional properties:
 * mongodb.collectionHeader -- name of the event header used to select the
 * target collection
 * mongodb.collectionMap.&lt;headerValue&gt; -- maps a header value to an
 * actual collection name
 * mongodb.collectionMapFallback -- whether to fall back to the default
 * collection when the header value has no mapping (default true)
 * mongodb.includeHeaders -- whether to include the Flume event headers in
 * the resulting document (default false)
 * mongodb.writeConcern -- the write concern to use
 * batchSize -- how many events to process in one batch (default 100)
 */
public class MongoDbSink extends AbstractSink implements Configurable, BatchSizeSupported {

    private static final Logger logger = LogManager.getLogger(MongoDbSink.class);

    private String connectionUri;
    private String databaseName;
    private String defaultCollection;
    private String collectionHeader;
    private Map<String, String> collectionMap;
    private boolean collectionMapFallback;
    private boolean includeHeaders;
    private int batchSize;
    private WriteConcern writeConcern;

    private MongoClient mongoClient;
    private MongoDbWriter writer;
    private MongoDbSinkCounter counter;

    // For testing
    public String getDatabaseName() {
        return databaseName;
    }

    public String getDefaultCollection() {
        return defaultCollection;
    }

    /** For testing: number of events skipped due to duplicate keys. */
    public long getDuplicateEventCount() {
        return counter.getDuplicateEventCount();
    }

    @Override
    public long getBatchSize() {
        return batchSize;
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;

        try {
            long processedEvents = 0;

            transaction = channel.getTransaction();
            transaction.begin();

            // Preserve insertion order per collection but batch by target collection
            // so that a single insertMany() call can be issued per collection.
            Map<String, List<Document>> documentsByCollection = new LinkedHashMap<>();

            for (; processedEvents < batchSize; processedEvents += 1) {
                Event event = channel.take();

                if (event == null) {
                    // no events available in channel
                    if (processedEvents == 0) {
                        result = Status.BACKOFF;
                        counter.incrementBatchEmptyCount();
                    } else {
                        counter.incrementBatchUnderflowCount();
                    }
                    break;
                }
                counter.incrementEventDrainAttemptCount();

                String targetCollection = resolveCollection(event);
                Document document = toDocument(event);

                documentsByCollection
                        .computeIfAbsent(targetCollection, k -> new ArrayList<>())
                        .add(document);
            }

            long insertedEvents = 0;
            long duplicateEvents = 0;
            for (Map.Entry<String, List<Document>> entry : documentsByCollection.entrySet()) {
                MongoDbWriteResult writeResult = writer.write(entry.getKey(), entry.getValue());
                insertedEvents += writeResult.getInsertedCount();
                duplicateEvents += writeResult.getDuplicateCount();
            }

            if (insertedEvents > 0) {
                counter.addToEventDrainSuccessCount(insertedEvents);
            }
            if (duplicateEvents > 0) {
                counter.addToDuplicateEventCount(duplicateEvents);
            }

            transaction.commit();
        } catch (Exception ex) {
            String errorMsg = "Failed to publish events";
            logger.error(errorMsg, ex);
            counter.incrementEventWriteOrChannelFail(ex);
            if (transaction != null) {
                transaction.rollback();
            }
            throw new EventDeliveryException(errorMsg, ex);
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }

        return result;
    }

    /**
     * Determines the collection an event should be written to. If a
     * collection header is configured and the event has a value for that
     * header, the value is looked up in the collection map to obtain the
     * actual collection name. If there is no mapping for the value,
     * {@link #collectionMapFallback} controls whether the default collection
     * is used or an exception is thrown. If no collection header is
     * configured, or the event has no value for it, the default collection
     * is used.
     */
    private String resolveCollection(Event event) throws EventDeliveryException {
        if (collectionHeader == null) {
            return defaultCollection;
        }
        String headerValue = event.getHeaders().get(collectionHeader);
        if (headerValue == null) {
            return defaultCollection;
        }
        String mappedCollection = collectionMap.get(headerValue);
        if (mappedCollection != null) {
            return mappedCollection;
        }
        if (collectionMapFallback) {
            logger.debug(
                    "No collection mapping found for header {}={}. Using default collection {}",
                    collectionHeader,
                    headerValue,
                    defaultCollection);
            return defaultCollection;
        }
        throw new EventDeliveryException(
                "No collection mapping found for header " + collectionHeader + "=" + headerValue);
    }

    private Document toDocument(Event event) {
        Document document;
        String body = new String(event.getBody(), StandardCharsets.UTF_8);
        try {
            document = Document.parse(body);
        } catch (Exception ex) {
            document = new Document(MongoDbSinkConstants.BODY_FIELD, body);
        }
        if (includeHeaders) {
            document.append(MongoDbSinkConstants.HEADERS_FIELD, new Document(event.getHeaders()));
        }
        return document;
    }

    @Override
    public synchronized void start() {
        mongoClient = MongoClients.create(connectionUri);
        MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
        writer = new DefaultMongoDbWriter(mongoDatabase, writeConcern);
        counter.start();
        super.start();
    }

    @Override
    public synchronized void stop() {
        try {
            if (writer != null) {
                writer.close();
            }
            if (mongoClient != null) {
                mongoClient.close();
            }
        } catch (MongoException ex) {
            logger.warn("Error closing MongoDB client", ex);
        }
        counter.stop();
        logger.info("MongoDb Sink {} stopped. Metrics: {}", getName(), counter);
        super.stop();
    }

    @Override
    public void configure(Context context) {
        connectionUri = context.getString(MongoDbSinkConstants.CONNECTION_URI);
        if (connectionUri == null || connectionUri.isEmpty()) {
            throw new ConfigurationException("mongodb.uri must be specified");
        }

        databaseName = context.getString(MongoDbSinkConstants.DATABASE_NAME);
        if (databaseName == null || databaseName.isEmpty()) {
            throw new ConfigurationException("mongodb.database must be specified");
        }

        defaultCollection = context.getString(MongoDbSinkConstants.COLLECTION);
        if (defaultCollection == null || defaultCollection.isEmpty()) {
            throw new ConfigurationException("mongodb.collection must be specified");
        }

        collectionHeader = context.getString(MongoDbSinkConstants.COLLECTION_HEADER);
        collectionMap = context.getSubProperties(MongoDbSinkConstants.COLLECTION_MAP_PREFIX);
        collectionMapFallback = context.getBoolean(
                MongoDbSinkConstants.COLLECTION_MAP_FALLBACK, MongoDbSinkConstants.DEFAULT_COLLECTION_MAP_FALLBACK);

        if (collectionHeader != null && logger.isDebugEnabled()) {
            logger.debug(
                    "Using header {} with mappings {} to select target collection", collectionHeader, collectionMap);
        }

        includeHeaders =
                context.getBoolean(MongoDbSinkConstants.INCLUDE_HEADERS, MongoDbSinkConstants.DEFAULT_INCLUDE_HEADERS);

        batchSize = context.getInteger(MongoDbSinkConstants.BATCH_SIZE, MongoDbSinkConstants.DEFAULT_BATCH_SIZE);

        String writeConcernName = context.getString(MongoDbSinkConstants.WRITE_CONCERN);
        if (writeConcernName != null && !writeConcernName.isEmpty()) {
            writeConcern = WriteConcern.valueOf(writeConcernName);
            if (writeConcern == null) {
                throw new ConfigurationException("Unknown write concern: " + writeConcernName);
            }
        }

        if (counter == null) {
            counter = new MongoDbSinkCounter(getName());
        }
    }
}
