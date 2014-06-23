grails.project.work.dir = 'target'

grails.project.dependency.resolution = {
    // inherit Grails' default dependencies
    inherits("global") {
        // uncomment to disable ehcache
        // excludes 'ehcache'
    }
    log "warn" // log level of Ivy resolver, either 'error', 'warn', 'info', 'debug' or 'verbose'
    repositories {
        grailsCentral()
        mavenLocal()
        mavenCentral()
    }
    dependencies {
      compile "org.elasticsearch:elasticsearch:1.0.1"        
    }

    plugins {
        build(":release:3.0.1",
              ":rest-client-builder:1.0.3",
              ":tomcat:7.0.47") {
            export = false
        }
        runtime(":hibernate:3.6.10.6") {
            export = false
        }
        // plugins needed at runtime but not for compilation
        runtime":time-series:0.2.4.1"
        runtime ":executor:0.3" 
        compile ":elasticsearch:0.90.3.0-SNAPSHOT"

        test ":executor:0.3" 
        test ":elasticsearch:0.90.3.0-SNAPSHOT"
    }
}

//if (System.getProperty('grails.project.plugins.dir')) {
    grails.project.plugins.dir=System.getProperty('grails.project.plugins.dir', 'plugins')
//}
