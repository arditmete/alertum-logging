import com.google.protobuf.gradle.*

plugins {
    kotlin("jvm") version "1.9.25"
    id("com.google.protobuf") version "0.9.4"
    `maven-publish`
    signing
}

group = "io.github.arditmete"
version = "1.0.39"

repositories {
    mavenCentral()
}

java {
    withSourcesJar()
    withJavadocJar()
}
java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}
kotlin {
    jvmToolchain(17)
}
dependencies {
    implementation("ch.qos.logback:logback-classic:1.4.14")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.17.2")
    implementation("io.grpc:grpc-netty-shaded:1.64.0")
    implementation("io.grpc:grpc-protobuf:1.64.0")
    implementation("io.grpc:grpc-stub:1.64.0")
    implementation("javax.annotation:javax.annotation-api:1.3.2")
    implementation("com.google.protobuf:protobuf-java:3.25.3")
    compileOnly("org.springframework.boot:spring-boot-autoconfigure:3.2.5")
    compileOnly("org.springframework:spring-web:6.1.6")
    compileOnly("jakarta.servlet:jakarta.servlet-api:6.0.0")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.25.3"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.64.0"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                id("grpc")
            }
        }
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])

            artifactId = "alertum-logging"

            pom {
                name.set("Alertum Logging")
                description.set("Logging SDK for Alertum platform")
                url.set("https://github.com/arditmete/alertum-logging")

                licenses {
                    license {
                        name.set("MIT License")
                        url.set("https://opensource.org/licenses/MIT")
                    }
                }

                developers {
                    developer {
                        id.set("arditmete")
                        name.set("Ardit Mete")
                        email.set("product@alertum.co")
                    }
                }

                scm {
                    url.set("https://github.com/arditmete/alertum-logging")
                    connection.set("scm:git:git://github.com/arditmete/alertum-logging.git")
                    developerConnection.set("scm:git:ssh://github.com/arditmete/alertum-logging.git")
                }
            }
        }
    }

    repositories {
        maven {
            name = "bundleRepo"
            url = uri(layout.buildDirectory.dir("repo"))
        }
    }
}

signing {
    useGpgCmd()
    sign(publishing.publications)
}

/**
 * Bundle required by Maven Central Portal
 */
tasks.register<Zip>("bundleRelease") {
    dependsOn("publishAllPublicationsToBundleRepoRepository")

    from(layout.buildDirectory.dir("repo"))

    archiveFileName.set("bundle.zip")
    destinationDirectory.set(layout.buildDirectory.dir("bundle"))
}
