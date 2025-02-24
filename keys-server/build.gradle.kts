/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("buildlogic.java-application-conventions")

    id("io.freefair.aspectj.post-compile-weaving") version "8.12.2" // Used for aspects
}

dependencies {
    implementation(project(":api"))
    implementation(project(":server-base"))
    implementation(project(":database-utils"))
    implementation(libs.dropwizard.core)
    implementation(libs.commons.codec)
    testImplementation(libs.dropwizard.testing)

    implementation(libs.bundles.logging)

    // Database
    implementation(libs.liquibase.core)
    implementation(libs.bundles.jdbi)
    testImplementation(libs.bundles.jdbi.testing)
    testImplementation(libs.hsqldb)

    // PreCompile
    implementation(libs.javax.inject)
    implementation(libs.dagger)
    annotationProcessor(libs.dagger.compiler)
    implementation(libs.immutables.annotations)
    annotationProcessor(libs.immutables.value)
    testAnnotationProcessor(libs.dagger.compiler)
    testAnnotationProcessor(libs.immutables.value)

    // Metrics
    implementation(libs.micrometer.core)
    implementation(libs.codehead.metrics)
    implementation(libs.codehead.metrics.micrometer)
    implementation(libs.codehead.metrics.declarative)
    aspect(libs.codehead.metrics.declarative)
    testImplementation(libs.codehead.metrics.test)

    testImplementation(libs.bundles.testing)
    testImplementation(libs.codehead.test)
}

application {
    mainClass.set("com.codeheadsystems.keys.KeysServer")
}
