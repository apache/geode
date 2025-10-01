/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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

package org.apache.geode.gradle.sbom

import groovy.json.JsonSlurper

/**
 * Utility class for validating SBOM (Software Bill of Materials) content and format.
 * This supports PR 3: Basic SBOM Generation for Single Module validation requirements.
 * 
 * Provides validation for:
 * - CycloneDX format compliance
 * - Dependency accuracy and completeness
 * - License information validation
 * - Component metadata verification
 * - SBOM structure and required fields
 */
class SbomValidationUtils {

    /**
     * Validates that an SBOM file complies with CycloneDX format requirements.
     * 
     * @param sbomFile The SBOM JSON file to validate
     * @return ValidationResult containing validation status and details
     */
    static ValidationResult validateCycloneDxFormat(File sbomFile) {
        def result = new ValidationResult()
        
        if (!sbomFile.exists()) {
            result.addError("SBOM file does not exist: ${sbomFile.absolutePath}")
            return result
        }
        
        try {
            def sbomContent = new JsonSlurper().parse(sbomFile)
            
            // Validate required top-level fields
            validateRequiredField(result, sbomContent, 'bomFormat', 'CycloneDX')
            validateRequiredField(result, sbomContent, 'specVersion')
            validateRequiredField(result, sbomContent, 'serialNumber')
            validateRequiredField(result, sbomContent, 'version')
            validateRequiredField(result, sbomContent, 'metadata')
            validateRequiredField(result, sbomContent, 'components')
            
            // Validate spec version format
            if (sbomContent.specVersion && !(sbomContent.specVersion ==~ /^1\.\d+$/)) {
                result.addError("Invalid specVersion format: ${sbomContent.specVersion}")
            }
            
            // Validate serial number format (should be URN UUID)
            if (sbomContent.serialNumber && !(sbomContent.serialNumber ==~ /^urn:uuid:[0-9a-f-]{36}$/)) {
                result.addError("Invalid serialNumber format: ${sbomContent.serialNumber}")
            }
            
            // Validate metadata structure
            if (sbomContent.metadata) {
                validateMetadata(result, sbomContent.metadata)
            }
            
            // Validate components structure
            if (sbomContent.components) {
                validateComponents(result, sbomContent.components)
            }
            
            if (result.isValid()) {
                result.addInfo("SBOM format validation passed")
                result.addInfo("Spec version: ${sbomContent.specVersion}")
                result.addInfo("Components count: ${sbomContent.components?.size() ?: 0}")
            }
            
        } catch (Exception e) {
            result.addError("Failed to parse SBOM JSON: ${e.message}")
        }
        
        return result
    }
    
    /**
     * Validates that SBOM contains expected dependencies and excludes test dependencies.
     * 
     * @param sbomFile The SBOM JSON file to validate
     * @param expectedDependencies List of expected dependency names
     * @param excludedDependencies List of dependencies that should not be present
     * @return ValidationResult containing validation status and details
     */
    static ValidationResult validateDependencyAccuracy(File sbomFile, 
                                                      List<String> expectedDependencies = [], 
                                                      List<String> excludedDependencies = []) {
        def result = new ValidationResult()
        
        if (!sbomFile.exists()) {
            result.addError("SBOM file does not exist: ${sbomFile.absolutePath}")
            return result
        }
        
        try {
            def sbomContent = new JsonSlurper().parse(sbomFile)
            def componentNames = sbomContent.components?.collect { it.name } ?: []
            
            // Check expected dependencies
            expectedDependencies.each { expectedDep ->
                if (componentNames.any { it.contains(expectedDep) }) {
                    result.addInfo("✅ Expected dependency found: ${expectedDep}")
                } else {
                    result.addError("❌ Expected dependency missing: ${expectedDep}")
                }
            }
            
            // Check excluded dependencies
            excludedDependencies.each { excludedDep ->
                def foundExcluded = componentNames.findAll { it.contains(excludedDep) }
                if (foundExcluded.isEmpty()) {
                    result.addInfo("✅ Excluded dependency correctly absent: ${excludedDep}")
                } else {
                    result.addError("❌ Excluded dependency found: ${foundExcluded}")
                }
            }
            
            result.addInfo("Total components in SBOM: ${componentNames.size()}")
            
        } catch (Exception e) {
            result.addError("Failed to validate dependencies: ${e.message}")
        }
        
        return result
    }
    
    /**
     * Validates license information in SBOM components.
     * 
     * @param sbomFile The SBOM JSON file to validate
     * @return ValidationResult containing validation status and details
     */
    static ValidationResult validateLicenseInformation(File sbomFile) {
        def result = new ValidationResult()
        
        if (!sbomFile.exists()) {
            result.addError("SBOM file does not exist: ${sbomFile.absolutePath}")
            return result
        }
        
        try {
            def sbomContent = new JsonSlurper().parse(sbomFile)
            def components = sbomContent.components ?: []
            
            int componentsWithLicenses = 0
            int totalComponents = components.size()
            
            components.each { component ->
                if (component.licenses && !component.licenses.isEmpty()) {
                    componentsWithLicenses++
                    result.addInfo("Component ${component.name} has license information")
                } else {
                    result.addWarning("Component ${component.name} missing license information")
                }
            }
            
            def licensePercentage = totalComponents > 0 ? 
                (componentsWithLicenses / totalComponents) * 100 : 0
            
            result.addInfo("License coverage: ${componentsWithLicenses}/${totalComponents} (${licensePercentage.round(1)}%)")
            
            if (licensePercentage >= 80) {
                result.addInfo("✅ Good license coverage (≥80%)")
            } else if (licensePercentage >= 50) {
                result.addWarning("⚠️ Moderate license coverage (50-79%)")
            } else {
                result.addError("❌ Poor license coverage (<50%)")
            }
            
        } catch (Exception e) {
            result.addError("Failed to validate license information: ${e.message}")
        }
        
        return result
    }
    
    /**
     * Validates component versions match expected patterns and are not empty.
     * 
     * @param sbomFile The SBOM JSON file to validate
     * @return ValidationResult containing validation status and details
     */
    static ValidationResult validateComponentVersions(File sbomFile) {
        def result = new ValidationResult()
        
        if (!sbomFile.exists()) {
            result.addError("SBOM file does not exist: ${sbomFile.absolutePath}")
            return result
        }
        
        try {
            def sbomContent = new JsonSlurper().parse(sbomFile)
            def components = sbomContent.components ?: []
            
            components.each { component ->
                if (!component.version || component.version.trim().isEmpty()) {
                    result.addError("Component ${component.name} missing version")
                } else if (component.version == 'unspecified' || component.version == 'unknown') {
                    result.addWarning("Component ${component.name} has unspecified version")
                } else {
                    result.addInfo("Component ${component.name} version: ${component.version}")
                }
                
                // Validate PURL (Package URL) if present
                if (component.purl) {
                    if (component.purl.startsWith('pkg:')) {
                        result.addInfo("Component ${component.name} has valid PURL")
                    } else {
                        result.addError("Component ${component.name} has invalid PURL format")
                    }
                }
            }
            
        } catch (Exception e) {
            result.addError("Failed to validate component versions: ${e.message}")
        }
        
        return result
    }
    
    private static void validateRequiredField(ValidationResult result, def sbomContent, String fieldName, String expectedValue = null) {
        if (!sbomContent.containsKey(fieldName)) {
            result.addError("Missing required field: ${fieldName}")
        } else if (expectedValue && sbomContent[fieldName] != expectedValue) {
            result.addError("Field ${fieldName} has incorrect value. Expected: ${expectedValue}, Got: ${sbomContent[fieldName]}")
        }
    }
    
    private static void validateMetadata(ValidationResult result, def metadata) {
        if (!metadata.timestamp) {
            result.addError("Missing metadata.timestamp")
        }
        
        if (!metadata.component) {
            result.addError("Missing metadata.component")
        } else {
            if (!metadata.component.type) {
                result.addError("Missing metadata.component.type")
            }
            if (!metadata.component.name) {
                result.addError("Missing metadata.component.name")
            }
        }
    }
    
    private static void validateComponents(ValidationResult result, def components) {
        if (!(components instanceof List)) {
            result.addError("Components should be an array")
            return
        }
        
        components.eachWithIndex { component, index ->
            if (!component.type) {
                result.addError("Component ${index} missing type")
            }
            if (!component.name) {
                result.addError("Component ${index} missing name")
            }
            if (!component.version) {
                result.addError("Component ${index} missing version")
            }
        }
    }
    
    /**
     * Container for validation results with errors, warnings, and info messages.
     */
    static class ValidationResult {
        private List<String> errors = []
        private List<String> warnings = []
        private List<String> info = []
        
        void addError(String message) {
            errors.add(message)
        }
        
        void addWarning(String message) {
            warnings.add(message)
        }
        
        void addInfo(String message) {
            info.add(message)
        }
        
        boolean isValid() {
            return errors.isEmpty()
        }
        
        List<String> getErrors() {
            return errors.asImmutable()
        }
        
        List<String> getWarnings() {
            return warnings.asImmutable()
        }
        
        List<String> getInfo() {
            return info.asImmutable()
        }
        
        String getSummary() {
            def summary = []
            if (!errors.isEmpty()) {
                summary.add("❌ ${errors.size()} error(s)")
            }
            if (!warnings.isEmpty()) {
                summary.add("⚠️ ${warnings.size()} warning(s)")
            }
            if (!info.isEmpty()) {
                summary.add("ℹ️ ${info.size()} info message(s)")
            }
            return summary.join(", ") ?: "✅ No issues"
        }
        
        @Override
        String toString() {
            def result = ["=== SBOM Validation Result ==="]
            result.add("Status: ${isValid() ? '✅ VALID' : '❌ INVALID'}")
            result.add("Summary: ${getSummary()}")
            
            if (!errors.isEmpty()) {
                result.add("\nErrors:")
                errors.each { result.add("  - ${it}") }
            }
            
            if (!warnings.isEmpty()) {
                result.add("\nWarnings:")
                warnings.each { result.add("  - ${it}") }
            }
            
            if (!info.isEmpty()) {
                result.add("\nInfo:")
                info.each { result.add("  - ${it}") }
            }
            
            result.add("=== End Validation Result ===")
            return result.join("\n")
        }
    }
}
