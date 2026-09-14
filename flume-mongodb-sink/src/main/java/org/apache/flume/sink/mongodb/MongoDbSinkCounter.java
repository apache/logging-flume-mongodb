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

import org.apache.flume.instrumentation.SinkCounter;

/**
 * {@link SinkCounter} extension that additionally tracks the number of
 * events skipped because they duplicated a document already present in
 * MongoDB (i.e. resulted in a {@code com.mongodb.DuplicateKeyException}).
 * These events are not counted towards {@code eventDrainSuccessCount} since
 * they were not actually inserted, but they should also not be treated as a
 * batch failure.
 */
public class MongoDbSinkCounter extends SinkCounter {

    private static final String COUNTER_DUPLICATE_EVENT = "sink.event.duplicate";

    private static final String[] ATTRIBUTES = {COUNTER_DUPLICATE_EVENT};

    public MongoDbSinkCounter(String name) {
        super(name, ATTRIBUTES);
    }

    public long getDuplicateEventCount() {
        return get(COUNTER_DUPLICATE_EVENT);
    }

    public long incrementDuplicateEventCount() {
        return increment(COUNTER_DUPLICATE_EVENT);
    }

    public long addToDuplicateEventCount(long delta) {
        return addAndGet(COUNTER_DUPLICATE_EVENT, delta);
    }
}
