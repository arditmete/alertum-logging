plugins {
    kotlin("jvm") version "1.9.25"
    `maven-publish`
    signing
}

group = "io.alertum"
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
    repositories {
        maven {
            name = "sonatype"
            url = uri(
                    if (version.toString().endsWith("SNAPSHOT"))
                        "https://s01.oss.sonatype.org/content/repositories/snapshots/"
                    else
                        "https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/"
            )
            credentials {
                username = System.getenv("OSSRH_USERNAME")
                password = System.getenv("OSSRH_PASSWORD")
            }
        }
    }
}

signing {
    useGpgCmd()
    sign(publishing.publications)
}

tasks.register<Zip>("bundleRelease") {
    dependsOn("publishToMavenLocal")

    from(layout.buildDirectory.dir("publications/mavenJava"))
    archiveFileName.set("bundle.zip")
    destinationDirectory.set(layout.buildDirectory.dir("bundle"))
}