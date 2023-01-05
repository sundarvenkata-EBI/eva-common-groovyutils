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

import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.CriteriaDefinition

class RetryableCursor<T> implements Iterable<T> {
    private RetryableBatchingCursor<T> batchingCursor
    Iterator<T> currResultListIterator = null

    // Need this to satisfy Spring gods who feast on empty constructors
    RetryableCursor() {}

    /***
     * A retryable cursor on top of MongoCursor that returns single entities of type T
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
    RetryableCursor(CriteriaDefinition filterCriteria, MongoTemplate mongoTemplate, Class<T> collectionClass,
                    int batchSize = 1000, String collectionName = null) {
        this.batchingCursor = new RetryableBatchingCursor<>(filterCriteria, mongoTemplate, collectionClass,
                batchSize, collectionName)
    }

    @Override
    Iterator<T> iterator() {
        def currObject = this
        return new Iterator<T>() {
            @Override
            boolean hasNext() {
                // We need this somewhat convoluted method to
                // unwind the "list of results" returned by the batching cursor
                if (Objects.isNull(currObject.currResultListIterator) || !currObject.currResultListIterator.hasNext()) {
                    if (currObject.batchingCursor.resultIterator.hasNext()) {
                        currObject.currResultListIterator = currObject.batchingCursor.resultIterator.next().iterator()
                    }
                }
                return Objects.isNull(currObject.currResultListIterator)?
                        false:currObject.currResultListIterator.hasNext()
            }

            @Override
            T next() {
                return currObject.currResultListIterator.next()
            }
        }
    }
}
