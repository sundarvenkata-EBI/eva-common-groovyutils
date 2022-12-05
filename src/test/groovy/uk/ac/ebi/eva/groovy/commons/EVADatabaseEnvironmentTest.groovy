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

import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import uk.ac.ebi.eva.accession.core.GenericApplication
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity

import static org.junit.Assert.assertEquals
import static uk.ac.ebi.eva.groovy.commons.CommonTestUtils.createRS
import static uk.ac.ebi.eva.groovy.commons.CommonTestUtils.createSS

@RunWith(SpringRunner.class)
@TestPropertySource(CommonTestUtils.testPropertiesFilePath)
class EVADatabaseEnvironmentTest {
    @Autowired
    private ApplicationContext applicationContext

    private boolean dbEnvSetupDone = false

    private static final String ASSEMBLY = "GCA_000000001.1"

    private static final int TAXONOMY = 60711

    private static EVADatabaseEnvironment dbEnv

    private static void cleanup() {
        dbEnv.mongoClient.dropDatabase(dbEnv.mongoTemplate.db.name)
    }

    @Before
    void setUp() {
        if (!this.dbEnvSetupDone) {
            // We need to directly use the application-test.properties file here
            // because there are symbolic variables like |eva.mongo.host.test|
            // that needs to be resolved by Spring and Maven dynamically
            def dynamicPropertyFilePath = CommonTestUtils.getTempPropertyFilePath(applicationContext)
            dbEnv = EVADatabaseEnvironment.createFromSpringContext(dynamicPropertyFilePath, GenericApplication.class)
            this.dbEnvSetupDone = true
        }
        cleanup()
    }

    @After
    void tearDown() {
        cleanup()
    }

    @Test
    void testEVADatabaseEnvironment() {
        assertEquals("evaGroovyUtilsTest", dbEnv.mongoTemplate.db.name)
        assertEquals(ReadPreference.primary(), dbEnv.mongoClient.readPreference)
        assertEquals(WriteConcern.MAJORITY, dbEnv.mongoClient.writeConcern)
        assertEquals(ReadConcern.MAJORITY, dbEnv.mongoClient.readConcern)
        // Ensure that other miscellaneous Spring Beans in the environment are accessible via the application context
        // Start accession for EVA accessioned SS
        assertEquals(5e9.toLong(), dbEnv.springApplicationContext.getBean("accessioningMonotonicInitSs"))
        // Start accession for EVA accessioned RS
        assertEquals(3e9.toLong(), dbEnv.springApplicationContext.getBean("accessioningMonotonicInitRs"))

        def allSS = (1L..10L).collect{createSS(ASSEMBLY, TAXONOMY, 5e9.toLong() + it,
                3e9.toLong() + it, 100+it, "C", "G")}
        allSS.each{dbEnv.mongoTemplate.insert(it)}
        (1L..10L).each {ssAccessionOffset ->
            assertEquals(3e9.toLong() + ssAccessionOffset,
                    dbEnv.submittedVariantAccessioningService.getByAccession(5e9.toLong() + ssAccessionOffset)
                            .data.clusteredVariantAccession)
        }

        def allRS = (1L..10L).collect{createRS(ASSEMBLY, TAXONOMY, 3e9.toLong() + it, 100L + it)}
        allRS.each{dbEnv.mongoTemplate.insert(it)}
        (1L..10L).each {rsAccessionOffset ->
            assertEquals(3e9.toLong() + rsAccessionOffset,
                    dbEnv.clusteredVariantAccessioningService.getByAccession(3e9.toLong() + rsAccessionOffset).accession)
        }
    }

    @Test
    void testBulkInsertIgnoreDuplicates() {
        def allSS = (1L..10L).collect{createSS(ASSEMBLY, TAXONOMY, 5e9.toLong() + it,
                3e9.toLong() + it, 100+it, "C", "G")}
        assertEquals(10, dbEnv.bulkInsertIgnoreDuplicates(allSS, SubmittedVariantEntity.class))
        // Ensure that invoking the bulkInsertIgnoreDuplicates the second time does not result in any inserts
        assertEquals(0, dbEnv.bulkInsertIgnoreDuplicates(allSS, SubmittedVariantEntity.class))
    }
}
