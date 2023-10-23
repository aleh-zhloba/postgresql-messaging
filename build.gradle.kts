import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
	id("org.springframework.boot") version "3.1.4"
	id("io.spring.dependency-management") version "1.1.3"

	kotlin("jvm") version "1.9.10"
	kotlin("plugin.spring") version "1.9.10"
	kotlin("plugin.serialization") version "1.9.10"
}

group = "com.github.aleh-zhloba"
version = "0.0.1-SNAPSHOT"

java {
	sourceCompatibility = JavaVersion.VERSION_17

	registerFeature("kotlinx") {
		usingSourceSet(sourceSets["main"])
	}
}

repositories {
	mavenCentral()
}

dependencies {
	compileOnly("org.springframework:spring-messaging")
	compileOnly("org.springframework:spring-context")
	compileOnly("org.springframework.boot:spring-boot-autoconfigure")

	compileOnly("com.fasterxml.jackson.core:jackson-databind")
	compileOnly("org.postgresql:r2dbc-postgresql:1.0.2.RELEASE")
	compileOnly("io.projectreactor.kotlin:reactor-kotlin-extensions:1.2.2")

	"kotlinxImplementation"("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.0")

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

tasks.withType<KotlinCompile> {
	kotlinOptions {
		freeCompilerArgs += "-Xjsr305=strict"
		jvmTarget = "17"
	}
}

tasks.withType<Test> {
	useJUnitPlatform()
}
