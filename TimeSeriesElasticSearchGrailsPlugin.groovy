class TimeSeriesElasticSearchGrailsPlugin {
    // the plugin version
    def version = "0.1"
    // the version or versions of Grails the plugin is designed for
    def grailsVersion = "2.3 > *"
    // resources that are excluded from plugin packaging
    def pluginExcludes = [
        "grails-app/views/error.gsp"
    ]

    // TODO Fill in these fields
    def title = "Time Series Elastic Search Plugin" // Headline display name of the plugin
    def author = "Jeremy Leng"
    def authorEmail = "jleng@bcap.com"
    def description = '''\
Elastic Search implementation of time series.
'''
    // URL to the plugin's documentation
    def documentation = "https://github.com/bertramdev/timeseries-elastic-search"

    // Extra (optional) plugin metadata

    // License: one of 'APACHE', 'GPL2', 'GPL3'
    def license = "APACHE"

    // Details of company behind the plugin (if there is one)
    def organization = [ name: "BertramLabs", url: "http://www.bertramlabs.com/" ]

    def dependsOn = [timeSeries: "* > 0.1-SNAPSHOT"]

    // Location of the plugin's issue tracker.
    def issueManagement = [ system: "GIT", url: "https://github.com/bertramdev/timeseries-elastic-search" ]

    // Online location of the plugin's browseable source code.
    def scm = [ url: "https://github.com/bertramdev/timeseries-elastic-search" ]

    def doWithWebDescriptor = { xml ->
        // TODO Implement additions to web.xml (optional), this event occurs before
    }

    def doWithSpring = {
        // TODO Implement runtime spring config (optional)
    }

    def doWithApplicationContext = { applicationContext ->
        def provider = new grails.plugins.timeseries.es.ElasticSearchTimeSeriesProvider()
        provider.addMetricMappingTemplate()
        addAsyncMethods(application, provider)
        applicationContext['timeSeriesService'].registerProvider(provider)
    }

    def doWithDynamicMethods = { ctx ->
    }

    def onChange = { event ->
        if(event.source == ctx.timeSeriesService.getProvider('es')) {
            addAsyncMethods(application, event.source)
        }
    }

    def addAsyncMethods(application, clazz) {
        clazz.metaClass.runAsync = { Runnable runme ->
            application.mainContext.executorService.withPersistence(runme)
        }
        clazz.metaClass.callAsync = { Closure clos ->
            application.mainContext.executorService.withPersistence(clos)
        }
        clazz.metaClass.callAsync = { Runnable runme, returnval ->
            application.mainContext.executorService.withPersistence(runme, returnval)
        }
    }

    def onConfigChange = { event ->
        // TODO Implement code that is executed when the project configuration changes.
        // The event is the same as for 'onChange'.
    }

    def onShutdown = { event ->
        // TODO Implement code that is executed when the application shuts down (optional)
    }
}
