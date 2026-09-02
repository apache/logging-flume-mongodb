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

import com.mongodb.DuplicateKeyException;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

/**
 * {@link MongoDbWriter} implementation backed by a real MongoDB
 * {@link MongoDatabase} connection.
 */
public class DefaultMongoDbWriter implements MongoDbWriter {

    private static final Logger logger = LogManager.getLogger(DefaultMongoDbWriter.class);

    private final MongoDatabase mongoDatabase;
    private final WriteConcern writeConcern;

    public DefaultMongoDbWriter(MongoDatabase mongoDatabase, WriteConcern writeConcern) {
        this.mongoDatabase = mongoDatabase;
        this.writeConcern = writeConcern;
    }

    @Override
    public MongoDbWriteResult write(String collectionName, List<Document> documents) {
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        if (writeConcern != null) {
            collection = collection.withWriteConcern(writeConcern);
        }

        long insertedCount = 0;
        long duplicateCount = 0;
        // Insert one document at a time (rather than insertMany) so that a
        // single duplicate key does not abort the rest of the batch.
        for (Document document : documents) {
            try {
                collection.insertOne(document);
                insertedCount++;
            } catch (DuplicateKeyException ex) {
                logger.warn(
                        "Duplicate key while inserting into collection {}, skipping event: {}",
                        collectionName,
                        ex.getMessage());
                duplicateCount++;
            } catch (MongoWriteException ex) {
                if (ex.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
                    logger.warn(
                            "Duplicate key while inserting into collection {}, skipping event: {}",
                            collectionName,
                            ex.getMessage());
                    duplicateCount++;
                } else {
                    throw ex;
                }
            }
        }
        return new MongoDbWriteResult(insertedCount, duplicateCount);
    }

    @Override
    public void close() {
        // The underlying MongoClient is owned and closed by MongoDbSink.
    }
}
