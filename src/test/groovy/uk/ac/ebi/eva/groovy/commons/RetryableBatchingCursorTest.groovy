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
import org.junit.After
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import uk.ac.ebi.eva.accession.core.GenericApplication
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity

import static org.junit.Assert.assertEquals
import static org.mockito.Mockito.RETURNS_DEEP_STUBS
import static uk.ac.ebi.eva.groovy.commons.CommonTestUtils.createSS

@RunWith(SpringRunner.class)
@TestPropertySource(CommonTestUtils.testPropertiesFilePath)
class RetryableBatchingCursorTest {

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
        def evaCursor = new RetryableBatchingCursor(new Criteria(), dbEnv.mongoTemplate, (Class) EVADatabaseEnvironment.sveClass)
        def numRecords = 0
        evaCursor.each {Object batch ->
            assertEquals(1, ((List) batch).size())
            numRecords += ((List) batch).size()
        }
        assertEquals(1, numRecords)

        dbEnv.mongoTemplate.insert(ss2)
        evaCursor = new RetryableBatchingCursor<SubmittedVariantEntity>(new Criteria(), dbEnv.mongoTemplate,
                (Class) EVADatabaseEnvironment.sveClass)
        numRecords = 0
        evaCursor.each {batch ->
            assertEquals(2, ((List) batch).size())
            numRecords += ((List) batch).size()
        }
        assertEquals(2, numRecords)

        remainingSS.each{dbEnv.mongoTemplate.insert(it)}
        evaCursor = new RetryableBatchingCursor<SubmittedVariantEntity>(new Criteria(), dbEnv.mongoTemplate,
                (Class) EVADatabaseEnvironment.sveClass, 3)
        numRecords = 0
        def batchIndex = 0
        evaCursor.each {batch ->
            if (batchIndex <= 2) {
                assertEquals(3, ((List) batch).size())
            } else {
                assertEquals(1, ((List) batch).size())
            }
            numRecords += ((List) batch).size()
            batchIndex += 1
        }
        assertEquals(10, numRecords)
    }

    @Test
    //TODO: Currently this test is very flaky and does not properly induce the error. Need a more reliable test.
    @Ignore
    void testEVACursorTimeouts() {
        def allSS = (1..100000).collect{createSS(ASSEMBLY, TAXONOMY, it, it+2, 100+it, "C", "G")}
        allSS.collate(1000).each{dbEnv.mongoTemplate.insert(it, EVADatabaseEnvironment.sveClass)}
        def evaCursor = new RetryableBatchingCursor(new Criteria(), dbEnv.mongoTemplate, (Class) EVADatabaseEnvironment.sveClass, 100)
        evaCursor.setRefreshInterval(30e3.toLong())
        evaCursor.each {it ->
            (1..100).each {
                dbEnv.mongoTemplate.findAll(EVADatabaseEnvironment.sveClass)
                println(it)
                Thread.sleep(100)
            }
            println(it)
        }
    }

    @Test
    //TODO: Currently this test mock is not injecting MongoCursorNotFoundException for some reason - should be fixed!
    @Ignore
    void testEVACursorRestarts() {
        def allSS = (1..10).collect{createSS(ASSEMBLY, TAXONOMY, it, it+2, 100+it, "C", "G")}
        allSS.each{dbEnv.mongoTemplate.insert(it)}
        def evaCursor = Mockito.mock(RetryableBatchingCursor<SubmittedVariantEntity>.class, RETURNS_DEEP_STUBS)
        def evaCursorIterator = Mockito.mock(RetryableBatchingCursorIterator.class, RETURNS_DEEP_STUBS)
        Mockito.when(evaCursor.iterator()).thenReturn(evaCursorIterator)
        Mockito.when(evaCursorIterator.hasNextResult())
                .thenReturn(true)
                .thenThrow(MongoCursorNotFoundException.class)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
        Mockito.when(evaCursorIterator.next())
                .thenReturn(allSS[0..2])
                .thenReturn(allSS[3..5])
                .thenReturn(allSS[6..8])
                .thenReturn(allSS[9..9])
        def numRecords = 0
        def batchIndex = 0
        evaCursor.forEach {batch ->
            if (batchIndex <= 2) {
                assertEquals(3, ((List) batch).size())
            } else {
                assertEquals(1, ((List) batch).size())
            }
            numRecords += ((List) batch).size()
            batchIndex += 1
        }
        assertEquals(4, batchIndex)
        assertEquals(10, numRecords)
    }
}
