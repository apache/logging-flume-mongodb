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

import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

/**
 * {@link MongoDbWriter} implementation backed by a real MongoDB
 * {@link MongoDatabase} connection.
 */
public class DefaultMongoDbWriter implements MongoDbWriter {

    private static final Logger logger = LogManager.getLogger(DefaultMongoDbWriter.class);

    /**
     * Unordered inserts let the server attempt every document of the batch.
     */
    private static final InsertManyOptions UNORDERED = new InsertManyOptions().ordered(false);

    /**
     * Matches the index name and key value in a server duplicate key message.
     */
    private static final Pattern DUPLICATE_KEY_MESSAGE = Pattern.compile("index:\\s+(\\S+)\\s+dup key:\\s+(\\{.*?\\})");

    private static final String UNKNOWN_INDEX_NAME = "<unknown index>";

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

        // Try to insert the whole batch in a single operation,
        // which is far more efficient than one insert per document.
        try {
            collection.insertMany(documents, UNORDERED);
            return new MongoDbWriteResult(documents.size(), 0);
        } catch (MongoBulkWriteException ex) {
            if (isDuplicateKeyOnly(ex)) {
                List<BulkWriteError> errors = ex.getWriteErrors();
                logDuplicates(collectionName, documents, errors);
                return new MongoDbWriteResult(documents.size() - errors.size(), errors.size());
            }
            throw ex;
        }
    }

    /**
     * Returns {@code true} if every error reported by the bulk write failure
     * is a duplicate key error.
     */
    private boolean isDuplicateKeyOnly(MongoBulkWriteException ex) {
        if (ex.getWriteConcernError() != null) {
            return false;
        }
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
     * Reports how many documents of the batch were rejected, broken down by
     * the unique index that rejected them, and logs the offending documents
     * themselves at debug level.
     */
    private void logDuplicates(String collectionName, List<Document> documents, List<BulkWriteError> errors) {
        Map<String, Integer> duplicatesByIndexName = new LinkedHashMap<>();
        for (BulkWriteError error : errors) {
            Matcher matcher = DUPLICATE_KEY_MESSAGE.matcher(error.getMessage());
            String indexName = matcher.find() ? matcher.group(1) : UNKNOWN_INDEX_NAME;
            duplicatesByIndexName.merge(indexName, 1, Integer::sum);
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Duplicate key in collection {} for the event at position {} of the batch: {}",
                        collectionName,
                        error.getIndex(),
                        documents.get(error.getIndex()).toJson());
            }
        }
        logger.warn(
                "Skipped {} of {} event(s) written to collection {} as duplicates, per unique index: {}",
                errors.size(),
                documents.size(),
                collectionName,
                duplicatesByIndexName);
    }

    @Override
    public void close() {
        // The underlying MongoClient is owned and closed by MongoDbSink.
    }
}
