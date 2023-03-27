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

import org.springframework.context.ApplicationContext
import org.springframework.core.env.MapPropertySource
import uk.ac.ebi.eva.accession.core.EVAObjectModelUtils
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity
import uk.ac.ebi.eva.commons.core.models.VariantType

class CommonTestUtils {
    static final String testPropertiesFilePath = "classpath:application-test.properties"
    static SubmittedVariantEntity createSS(String assembly, int taxonomy, Long ssAccession, Long rsAccession,
                                           Long start, String reference, String alternate) {

        SubmittedVariant sv = new SubmittedVariant(assembly, taxonomy, "PRJ1", "chr1", start, reference,
                alternate, rsAccession)
        return EVAObjectModelUtils.toSubmittedVariantEntity(ssAccession, sv)
    }

    static ClusteredVariantEntity createRS(String assembly, int taxonomy, Long rsAccession, Long start) {
        ClusteredVariant cv = new ClusteredVariant(assembly, taxonomy, "chr1", start, VariantType.SNV, true, null)
        return EVAObjectModelUtils.toClusteredVariantEntity(rsAccession, cv)
    }

    static String getTempPropertyFilePath(ApplicationContext applicationContext) {
        def tempFile = File.createTempFile("tempProps", ".properties")
        applicationContext.getEnvironment().propertySources.each { source ->
            if (source instanceof MapPropertySource) {
                source.getSource().each { key, value ->
                    tempFile.append("${key}=${value}\n")
                }
            }
        }
        return tempFile.getAbsolutePath()
    }
}
