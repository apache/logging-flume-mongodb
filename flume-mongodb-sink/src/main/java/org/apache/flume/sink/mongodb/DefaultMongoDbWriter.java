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
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteError;
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

        // First try to insert the whole batch in a single operation, which is
        // far more efficient than one insert per document. Only fall back to
        // inserting one document at a time if the batch insert fails because
        // of duplicate keys.
        try {
            collection.insertMany(documents);
            return new MongoDbWriteResult(documents.size(), 0);
        } catch (MongoBulkWriteException ex) {
            if (isDuplicateKeyOnly(ex)) {
                // insertMany() is ordered by default, so it stops at the first
                // failing document; everything before that point was already
                // successfully persisted. Skip those already-written documents
                // before retrying the remainder one at a time, otherwise they
                // would be re-attempted and incorrectly counted as duplicates.
                int alreadyInserted = ex.getWriteResult().getInsertedCount();
                logger.warn(
                        "Duplicate key(s) while batch inserting into collection {}, "
                                + "retrying remaining documents one at a time: {}",
                        collectionName,
                        ex.getMessage());
                MongoDbWriteResult retryResult = writeOneAtATime(
                        collection, collectionName, documents.subList(alreadyInserted, documents.size()));
                return new MongoDbWriteResult(
                        alreadyInserted + retryResult.getInsertedCount(), retryResult.getDuplicateCount());
            }
            throw ex;
        }
    }

    /**
     * Returns {@code true} if every error reported by the bulk write failure
     * is a duplicate key error.
     */
    private boolean isDuplicateKeyOnly(MongoBulkWriteException ex) {
        List<BulkWriteError> errors = ex.getWriteErrors();
        if (errors.isEmpty()) {
            return false;
        }
        for (BulkWriteError error : errors) {
            if (ErrorCategory.fromErrorCode(error.getCode()) != ErrorCategory.DUPLICATE_KEY) {
                return false;
            }
        }
        return true;
    }

    /**
     * Inserts documents one at a time so that a duplicate key on any single
     * document does not prevent the rest of the batch from being inserted.
     */
    private MongoDbWriteResult writeOneAtATime(
            MongoCollection<Document> collection, String collectionName, List<Document> documents) {
        long insertedCount = 0;
        long duplicateCount = 0;
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
