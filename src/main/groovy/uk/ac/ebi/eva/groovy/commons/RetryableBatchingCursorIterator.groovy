/*
 * Copyright 2022 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.groovy.commons

import com.mongodb.MongoCursorNotFoundException
import com.mongodb.client.MongoCursor

import org.bson.BsonDocument
import org.bson.Document
import org.slf4j.LoggerFactory
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.retry.RetryCallback
import org.springframework.retry.RetryContext
import org.springframework.retry.backoff.FixedBackOffPolicy
import org.springframework.retry.policy.SimpleRetryPolicy
import org.springframework.retry.support.RetryTemplate
import org.springframework.scheduling.TaskScheduler
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler

class RetryableBatchingCursorIterator<T> implements Iterator<List<T>> {
    BsonDocument serverSessionID
    MongoTemplate mongoTemplate
    Class<T> collectionClass
    MongoCursor<Document> mongoResultIterator
    int pageSize
    boolean startPeriodicRefreshThread = false
    // By default, the cursorTimeoutMillis on a Mongo server is 10 minutes
    // Therefore, try refreshing the cursors every 8 minutes (480e3 milliseconds)
    Long refreshInterval = 480e3.toLong()

    final static def logger = LoggerFactory.getLogger(RetryableBatchingCursorIterator.class)

    // Need this to satisfy Spring gods who feast on empty constructors
    RetryableBatchingCursorIterator() {}

    RetryableBatchingCursorIterator(BsonDocument serverSessionID, Class<T> collectionClass, MongoTemplate mongoTemplate,
                                    MongoCursor<Document> mongoResultIterator, int pageSize) {
        this.serverSessionID = serverSessionID
        this.collectionClass = collectionClass
        this.mongoTemplate = mongoTemplate
        this.mongoResultIterator = mongoResultIterator
        this.pageSize = pageSize
    }

    void setRefreshInterval (Long refreshInterval) {
        this.refreshInterval = refreshInterval
    }

    private void startPeriodicSessionRefresh() {
        TaskScheduler scheduler = new ThreadPoolTaskScheduler()
        scheduler.setPoolSize(3)
        scheduler.initialize()
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            void run() {
                this.mongoTemplate.db.runCommand(new Document("refreshSessions", Arrays.asList(this.serverSessionID)))
            }
        }, this.refreshInterval)
    }

    @Override
    boolean hasNext() {
        if (!startPeriodicRefreshThread) {
            startPeriodicSessionRefresh()
            startPeriodicRefreshThread = true
        }
        RetryTemplate retryTemplate = new RetryTemplate()

        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy()
        fixedBackOffPolicy.setBackOffPeriod(2000l)
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy)

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(5, [(MongoCursorNotFoundException.class): Boolean.TRUE])
        retryTemplate.setRetryPolicy(retryPolicy)

        return retryTemplate.execute(new RetryCallback<Boolean, Throwable>() {
            @Override
            Boolean doWithRetry(RetryContext context) throws Throwable {
                logger.debug("Retry count:" + context.retryCount)
                return this.hasNextResult()
            }
        })

    }

    // We only need this function because resultIterator cannot be mocked for testing because it is a final class
    boolean hasNextResult() {
        return this.mongoResultIterator.hasNext()
    }

    @Override
    List<T> next() {
        List<T> result = new ArrayList<>()
        result.add(this.mongoTemplate.converter.read(this.collectionClass, this.mongoResultIterator.next()))
        for (i in 0..this.pageSize-2) {
            if (this.mongoResultIterator.hasNext()) {
                result.add(this.mongoTemplate.converter.read(this.collectionClass, this.mongoResultIterator.next()))
            } else {
                break
            }
        }
        return result
    }
}
