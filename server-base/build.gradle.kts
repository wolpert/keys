/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("buildlogic.java-library-conventions")
}

dependencies {
    implementation(libs.dropwizard.core)

    implementation(libs.bundles.logging)
    implementation(libs.bundles.jackson)

    implementation(libs.javax.inject)
    implementation(libs.dagger)
    annotationProcessor(libs.dagger.compiler)
    implementation(libs.immutables.annotations)
    annotationProcessor(libs.immutables.value)

    implementation(libs.micrometer.core)
    implementation(libs.codehead.metrics)
    implementation(libs.codehead.metrics.micrometer)

    testImplementation(libs.dropwizard.testing)
    testImplementation(libs.bundles.logback)
    testImplementation(libs.bundles.testing)
    testImplementation(libs.codehead.test)
    testImplementation(libs.codehead.metrics.test)
    testAnnotationProcessor(libs.dagger.compiler)
    testAnnotationProcessor(libs.immutables.value)
}
