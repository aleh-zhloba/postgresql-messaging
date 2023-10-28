import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.springframework.boot.gradle.tasks.bundling.BootJar

plugins {
    id("org.springframework.boot") version "3.1.4"
    id("io.spring.dependency-management") version "1.1.3"

    kotlin("jvm") version "1.9.10"
    kotlin("plugin.spring") version "1.9.10"
    kotlin("plugin.serialization") version "1.9.10"

    id("maven-publish")
    id("java-library")
    id("signing")

    id("org.jmailen.kotlinter") version "4.0.0"
}

group = "io.github.aleh-zhloba"
version = "0.0.1-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_17

//    registerFeature("kotlinx") {
//        usingSourceSet(sourceSets["main"])
//    }

    withJavadocJar()
    withSourcesJar()
}

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.springframework:spring-messaging")
    compileOnly("org.springframework:spring-context")
    compileOnly("org.springframework.boot:spring-boot-autoconfigure")

    compileOnly("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")

    compileOnly("org.postgresql:r2dbc-postgresql:1.0.2.RELEASE")
    compileOnly("io.projectreactor.kotlin:reactor-kotlin-extensions:1.2.2")
    compileOnly("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.0")

    //"kotlinxImplementation"("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.0")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.springframework:spring-messaging")

    testImplementation("org.testcontainers:postgresql")
    testImplementation("org.testcontainers:r2dbc")
    testImplementation("org.testcontainers:junit-jupiter")

    testImplementation("com.fasterxml.jackson.core:jackson-databind")
    testImplementation("org.postgresql:r2dbc-postgresql:1.0.2.RELEASE")
    testImplementation("io.projectreactor:reactor-test")
    testImplementation("io.projectreactor.kotlin:reactor-kotlin-extensions:1.2.2")
}

tasks.named<Jar>("jar") {
    enabled = true
    archiveClassifier = ""
}

tasks.named<BootJar>("bootJar") {
    enabled = false
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs += "-Xjsr305=strict"
        jvmTarget = "17"
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}

kotlinter {
    ignoreFailures = false
    reporters = arrayOf("checkstyle", "plain")


}

tasks.check {
    dependsOn("installKotlinterPrePushHook")
}

publishing {
    repositories {
        maven {
            name = "deploy"
            url = uri("https://s01.oss.sonatype.org/content/repositories/snapshots/")
            credentials {
                username = "OSSRH_USERNAME".env()
                password = "OSSRH_PASSWORD".env()
            }
        }
    }

    publications {
        create<MavenPublication>("main") {
            pom {
                from(components["java"])
                name = "messaging-postgresql"
                description = "Convenient event bus for PostgreSQL-backed JVM applications"
                url = "https://github.com/aleh-zhloba/postgresql-messaging"
                licenses {
                    license {
                        name = "The Apache License, Version 2.0"
                        url = "http://www.apache.org/licenses/LICENSE-2.0.txt"
                    }
                }
                developers {
                    developer {
                        id = "aleh-zhloba"
                        name = "Aleh Zhloba"
                        email = "ol.zhloba@gmail.com"
                    }
                }
                scm {
                    connection = "scm:git:git://github.com/aleh-zhloba/postgresql-messaging.git"
                    developerConnection = "scm:git:ssh://github.com/aleh-zhloba/postgresql-messaging.git"
                    url = "https://github.com/aleh-zhloba/postgresql-messaging"
                }
            }
        }
    }
}

signing {
//    useInMemoryPgpKeys(
//        "SIGNING_KEY_ID".env(),
//        "SIGNING_KEY".env(),
//        "SIGNING_PASSWORD".env()
//    )
//    setRequired({
//        (project.extra["isReleaseVersion"] as Boolean) && gradle.taskGraph.hasTask("publish")
//    })
    sign(publishing.publications["main"])
}

fun String.env() = System.getenv(this) ?: ""
