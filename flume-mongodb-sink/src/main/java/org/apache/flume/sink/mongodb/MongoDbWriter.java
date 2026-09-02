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

import java.util.List;
import org.bson.Document;

/**
 * Abstraction over the MongoDB write path used by {@link MongoDbSink}.
 * Separating this out of the sink keeps the routing/document-construction
 * logic in {@link MongoDbSink} independent of, and easily testable without,
 * a live MongoDB connection.
 */
public interface MongoDbWriter {

    /**
     * Writes the given documents to the named collection. Documents that
     * fail to insert because they duplicate an existing document (i.e.
     * trigger a {@code com.mongodb.DuplicateKeyException}) are skipped
     * rather than causing the whole batch to fail; they are reported via
     * {@link MongoDbWriteResult#getDuplicateCount()}.
     *
     * @param collectionName the target collection name
     * @param documents      the documents to insert, in order
     * @return the number of documents inserted and the number skipped as
     *         duplicates
     */
    MongoDbWriteResult write(String collectionName, List<Document> documents);

    /**
     * Releases any resources (e.g. the underlying MongoDB client) held by
     * this writer.
     */
    void close();
}
