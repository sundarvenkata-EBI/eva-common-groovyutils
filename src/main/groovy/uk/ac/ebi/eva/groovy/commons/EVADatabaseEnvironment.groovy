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

import com.mongodb.MongoBulkWriteException
import com.mongodb.bulk.BulkWriteResult
import com.mongodb.MongoClient
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.core.env.PropertiesPropertySource
import org.springframework.dao.DuplicateKeyException
import org.springframework.data.mongodb.core.BulkOperations
import org.springframework.data.mongodb.core.MongoTemplate
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity
import uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantAccessioningService
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService

// This class is to create an environment for EVA databases from a given properties file
// Most of the code below is borrowed from MongoConfiguration in the eva-accession repository
class EVADatabaseEnvironment {
    MongoClient mongoClient
    MongoTemplate mongoTemplate
    SubmittedVariantAccessioningService submittedVariantAccessioningService
    ClusteredVariantAccessioningService clusteredVariantAccessioningService
    AnnotationConfigApplicationContext springApplicationContext

    static def sveClass = SubmittedVariantEntity.class
    static def dbsnpSveClass = DbsnpSubmittedVariantEntity.class
    static def svoeClass = SubmittedVariantOperationEntity.class
    static def dbsnpSvoeClass = DbsnpSubmittedVariantOperationEntity.class
    static def cveClass = ClusteredVariantEntity.class
    static def dbsnpCveClass = DbsnpClusteredVariantEntity.class
    static def cvoeClass = ClusteredVariantOperationEntity.class
    static def dbsnpCvoeClass = DbsnpClusteredVariantOperationEntity.class

    EVADatabaseEnvironment(MongoClient mongoClient, MongoTemplate mongoTemplate,
                           SubmittedVariantAccessioningService submittedVariantAccessioningService,
                           ClusteredVariantAccessioningService clusteredVariantAccessioningService,
                           AnnotationConfigApplicationContext springApplicationContext) {
        this.mongoClient = mongoClient
        this.mongoTemplate = mongoTemplate
        this.submittedVariantAccessioningService = submittedVariantAccessioningService
        this.clusteredVariantAccessioningService = clusteredVariantAccessioningService
        this.springApplicationContext = springApplicationContext
    }

    /***
     * Create a "bag of objects" pertinent to the EVA variant model
     * @param propertiesFile Application properties file (ex: application.properties)
     * that is used to run Spring-based pipelines
     * @param springApplicationClass Application class that should be wired (ex: if an environment for
     * accessioning pipeline is needed, use uk.ac.ebi.eva.depre)
     * @return A database Environment
     */
    static EVADatabaseEnvironment createFromSpringContext(String propertiesFile, Class springApplicationClass) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()
        def appProps = new Properties()
        appProps.load(new FileInputStream(new File(propertiesFile)))
        context.getEnvironment().getPropertySources().addLast(new PropertiesPropertySource("main", appProps))
        context.register(springApplicationClass)
        context.refresh()

        def mc = context.getBean(MongoClient.class)
        def mt = context.getBean(MongoTemplate.class)
        def sva = context.getBean(SubmittedVariantAccessioningService.class)
        def cva = context.getBean(ClusteredVariantAccessioningService.class)
        return new EVADatabaseEnvironment(mc, mt, sva, cva, context)
    }

    /***
     * Bulk insert records to a given collection in a Mongo database environment
     * @param recordsToInsert List of entities to insert
     * @param collectionClass Type of entity to insert
     * @param collectionName Explicit collection name if it is other than the pre-defined collection for collectionClass
     * (ex: submittedVariantEntity_custom)
     * @return Number of inserted documents
     */
    <T> int bulkInsertIgnoreDuplicates(List<T> recordsToInsert, Class<T> collectionClass,
                                       String collectionName = null) {
        if (recordsToInsert.size() > 0) {
            BulkOperations ops
            if (Objects.isNull(collectionName)) {
                ops = this.mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, collectionClass)
            } else {
                ops = this.mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, collectionClass, collectionName)
            }
            ops.insert(recordsToInsert)
            BulkWriteResult bulkWriteResult
            try {
                bulkWriteResult = ops.execute()
            }
            catch(DuplicateKeyException duplicateKeyException) {
                MongoBulkWriteException writeException = ((MongoBulkWriteException) duplicateKeyException.getCause())
                bulkWriteResult = writeException.getWriteResult()
            }
            return Objects.isNull(bulkWriteResult)? 0: bulkWriteResult.insertedCount
        }
        return 0
    }
}
