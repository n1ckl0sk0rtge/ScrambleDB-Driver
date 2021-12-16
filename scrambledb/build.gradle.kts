/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
plugins {
    java
    calcite.fmpp
    calcite.javacc
    id("com.github.vlsi.ide")
}

group = "org.apache.calcite"
version = "1.28.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    api(project(":core"))
    api(project(":linq4j"))
    api("org.apache.calcite.avatica:avatica-core:1.18.0")
    implementation("org.jboss.resteasy:resteasy-client:5.0.1.Final")
    implementation("org.jboss.resteasy:resteasy-jackson2-provider:5.0.1.Final")

    runtimeOnly("mysql:mysql-connector-java:8.0.26")

    testImplementation("mysql:mysql-connector-java:8.0.26")
    testImplementation(project(":core", "testClasses"))
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

val fmppMain by tasks.registering(org.apache.calcite.buildtools.fmpp.FmppTask::class) {
    inputs.dir("src/main/codegen")
    config.set(file("src/main/codegen/config.fmpp"))
    templates.set(file("$rootDir/core/src/main/codegen/templates"))
}

val javaCCMain by tasks.registering(org.apache.calcite.buildtools.javacc.JavaCCTask::class) {
    dependsOn(fmppMain)
    val parserFile = fmppMain.map {
        it.output.asFileTree.matching { include("**/Parser.jj") }
    }
    inputFile.from(parserFile)
    packageName.set("org.apache.calcite.sql.parser.scrambledb")
}

ide {
    fun generatedSource(javacc: TaskProvider<org.apache.calcite.buildtools.javacc.JavaCCTask>, sourceSet: String) =
        generatedJavaSources(javacc.get(), javacc.get().output.get().asFile, sourceSets.named(sourceSet))

    generatedSource(javaCCMain, "main")
}

val mainClass = "org.apache.calcite.jdbc.Driver"

val fatJar = task("fatJar", type = Jar::class) {
    archiveClassifier.set("all")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    manifest {
        attributes("Main-Class" to mainClass)
    }

    from(configurations.runtimeClasspath.get()
        .onEach { println("add from dependencies: ${it.name}") }
        .map { if (it.isDirectory) it else zipTree(it) }) {
        exclude("META-INF/*.SF")
        exclude("META-INF/*.DSA")
        exclude("META-INF/*.RSA")
        exclude("META-INF/LICENSE")
        exclude("META-INF/NOTICE")
    }

    val sourcesMain = sourceSets.main.get()
    sourcesMain.allSource.forEach { println("add from sources: ${it.name}") }
    from(sourcesMain.output)
}

tasks {
    "build" {
        dependsOn(fatJar)
    }
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}
