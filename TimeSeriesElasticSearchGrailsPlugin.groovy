class TimeSeriesElasticSearchGrailsPlugin {
    def version = "0.1.2"
    def grailsVersion = "2.0 > *"
    def title = "Time Series Elastic Search Plugin" // Headline display name of the plugin
    def author = "Jeremy Leng"
    def authorEmail = "jleng@bcap.com"
    def description = '''\
Elastic Search implementation of time series.
'''
    def documentation = "https://github.com/bertramdev/timeseries-elastic-search"
    def license = "APACHE"
    def organization = [ name: "BertramLabs", url: "http://www.bertramlabs.com/" ]
    def dependsOn = [timeSeries: "* > 0.1-SNAPSHOT"]
    def issueManagement = [ system: "GIT", url: "https://github.com/bertramdev/timeseries-elastic-search" ]
    def scm = [ url: "https://github.com/bertramdev/timeseries-elastic-search" ]
    def doWithApplicationContext = { applicationContext ->
        def provider = new grails.plugins.timeseries.es.ElasticSearchTimeSeriesProvider()
        provider.addMetricMappingTemplate()
        addAsyncMethods(application, provider)
        applicationContext['timeSeriesService'].registerProvider(provider)
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
}
