plugins {
    kotlin("jvm") version "1.9.25"
    `maven-publish`
    signing
}

group = "io.github.arditmete"
version = "1.0.1"

repositories {
    mavenCentral()
}

java {
    withSourcesJar()
    withJavadocJar()
}

dependencies {
    implementation("ch.qos.logback:logback-classic:1.4.14")
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