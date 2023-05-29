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

import com.mongodb.ClientSessionOptions
import com.mongodb.session.ClientSession
import org.bson.Document
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.CriteriaDefinition

class RetryableBatchingCursor<T> implements Iterable<T> {
    CriteriaDefinition filterCriteria
    MongoTemplate mongoTemplate
    String collectionName
    Class<T> collectionClass
    int batchSize
    RetryableBatchingCursorIterator<T> resultIterator

    // Need this to satisfy Spring gods who feast on empty constructors
    RetryableBatchingCursor() {}

    /***
     * A batching retryable cursor on top of MongoCursor that returns batches of results instead of single entities
     * @param filterCriteria Criteria to filter results for a given collectionClass (ex: where("rs").exists(false))
     * @param mongoTemplate MongoTemplate to use for querying
     * @param collectionClass Entity Class that should be used to return results (ex: SubmittedVariantEntity.class)
     * @param batchSize Optional: Number of results to return in each batch (default: 1000)
     * @param collectionName Optional: Name of specific collection to be queried. Supply this only if the entity
     * supplied in collectionClass does not have a corresponding "pre-bound" collection in the entity definition.
     * (ex: use submittedVariantEntity_custom if you wish to read SubmittedVariantEntity objects from a collection
     * named submittedVariantEntity_custom).
     * Note that this parameter is not needed if the collection name is submittedVariantEntity since that collection name is
     * already "pre-bound" to the SubmittedVariantEntity class in the entity definition.
     */
    RetryableBatchingCursor(CriteriaDefinition filterCriteria, MongoTemplate mongoTemplate, Class<T> collectionClass,
                            int batchSize = 1000, String collectionName = null) {
        this.filterCriteria = filterCriteria
        this.mongoTemplate = mongoTemplate
        this.collectionClass = collectionClass
        this.batchSize = batchSize
        this.collectionName = Objects.isNull(collectionName)? this.mongoTemplate.getCollectionName(this.collectionClass): collectionName

        ClientSessionOptions sessionOptions = ClientSessionOptions.builder()
                .causallyConsistent(true).build()

        ClientSession session = this.mongoTemplate.mongoDbFactory.getSession(sessionOptions)

        mongoTemplate.withSession(() -> session).execute { mongoOp ->
            def serverSessionID = session.serverSession.identifier
            def mongoIterator = mongoOp.getCollection(this.collectionName).find(
                    this.filterCriteria.criteriaObject).sort(new Document("_id", 1))
                    .noCursorTimeout(true).batchSize(batchSize).iterator()
//            def mongoIterator = mongoOp.getCollection(this.collectionName)
//                    .aggregate(session, Arrays.asList(Aggregates.match(this.filterCriteria.criteriaObject)),
//                            Document.class)
//                    .allowDiskUse(true).useCursor(true).batchSize(batchSize).iterator()
            this.resultIterator = new RetryableBatchingCursorIterator(serverSessionID, this.collectionClass,
                    this.mongoTemplate, mongoIterator, this.batchSize)
        }
    }

    @Override
    RetryableBatchingCursorIterator<T> iterator() {
        return this.resultIterator
    }

    void setRefreshInterval (Long refreshInterval) {
        this.resultIterator.setRefreshInterval(refreshInterval)
    }
}
