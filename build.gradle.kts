plugins {
    java
    alias(libs.plugins.jmh.run)
    alias(libs.plugins.jmh.report)
}

repositories {
    mavenCentral()
}

dependencies {
    // Use JUnit Jupiter for testing.
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    implementation(libs.duckdb)
    implementation(libs.flyway.core)
    runtimeOnly(libs.flyway.duckdb)
}

// Apply a specific Java toolchain to ease working on different environments.
java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

tasks.named<Test>("test") {
    // Use JUnit Platform for unit tests.
    useJUnitPlatform()
}

val jmhResultFile = project.layout.buildDirectory.file("results/jmh/results.json").get()
val jmhReportDir = project.layout.buildDirectory.dir("reports/jmh").get()

jmh {
    resultFormat = "json"
    resultsFile = jmhResultFile

    // DEBUG CONF
    warmupIterations = 2
    iterations = 4
    fork = 2
    timeOnIteration = "5s"
}

jmhReport {
    jmhResultPath = jmhResultFile.toString()
    jmhReportOutput = jmhReportDir.toString()
}

val jmhExecutionTask = tasks.named("jmh")

tasks.named("jmhReport") {
    // Ensure JMH has been executed before producing the report, otherwise launch it
    inputs.files(jmhExecutionTask.get().outputs.files)
    // Workaround: Must explicitly create output folder, otherwise an error is raised
    doFirst {
        jmhReportDir.asFile.mkdirs()
    }
}
