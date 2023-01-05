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

import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import uk.ac.ebi.eva.accession.core.GenericApplication
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity

import static org.junit.Assert.assertEquals
import static uk.ac.ebi.eva.groovy.commons.CommonTestUtils.createSS

@RunWith(SpringRunner.class)
@TestPropertySource(CommonTestUtils.testPropertiesFilePath)
class RetryableCursorTest {

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
    void testEVACursor() {
        def ss1 = createSS(ASSEMBLY, TAXONOMY, 1L, 1L, 100L, "C", "T")
        def ss2 = createSS(ASSEMBLY, TAXONOMY, 2L, 1L, 100L, "C", "A")
        def remainingSS = (3..10).collect{createSS(ASSEMBLY, TAXONOMY, it, it+2, 100+it, "C", "G")}

        dbEnv.mongoTemplate.insert(ss1)
        def evaCursor = new RetryableCursor(new Criteria(), dbEnv.mongoTemplate, (Class) EVADatabaseEnvironment.sveClass)
        def numRecords = evaCursor.collect().size()
        assertEquals(1, numRecords)

        dbEnv.mongoTemplate.insert(ss2)
        evaCursor = new RetryableCursor<SubmittedVariantEntity>(new Criteria(), dbEnv.mongoTemplate,
                (Class) EVADatabaseEnvironment.sveClass)
        numRecords = evaCursor.collect().size()
        assertEquals(2, numRecords)

        remainingSS.each{dbEnv.mongoTemplate.insert(it)}
        evaCursor = new RetryableCursor<SubmittedVariantEntity>(new Criteria(), dbEnv.mongoTemplate,
                (Class) EVADatabaseEnvironment.sveClass, 3)
        numRecords = evaCursor.collect().size()
        assertEquals(10, numRecords)
    }
}
