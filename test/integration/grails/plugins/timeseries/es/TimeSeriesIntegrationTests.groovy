package grails.plugins.timeseries.es

import groovy.util.GroovyTestCase
import grails.converters.*
import grails.plugins.timeseries.*

class TimeSeriesIntegrationTests extends GroovyTestCase {
	def transactional =  false
	def timeSeriesService
	def grailsApplication
	def setup() {
	}

	def cleanup() {
	}

	private flush(refId) {
		//timeSeriesService.getProvider('es').flush(refId)		
		//Thread.sleep(2000)
	}

	private getTestDate() {
		/*
		def c = new GregorianCalendar()
		c.set( Calendar.ERA, GregorianCalendar.AD )
		c.set( Calendar.YEAR, 2014 )
		c.set( Calendar.MONTH, Calendar.OCTOBER )
		c.set( Calendar.DATE, 31 )
		c.set( Calendar.HOUR, 10 )
		c.set( Calendar.SECOND, 31 )
		c.set( Calendar.MINUTE, 31 )
		c.set( Calendar.MILLISECOND, 0 )
		c.time
		*/
		new Date(System.currentTimeMillis() - (Long)(60*60*1000))
	}

	void testSaveMetrics() {
		flush('testSaveMetrics')
		def now = getTestDate()
		println now
		grailsApplication.config.grails.plugins.timeseries.poop.resolution = AbstractTimeSeriesProvider.ONE_SECOND
		timeSeriesService.saveMetric('testSaveMetrics', 'poop', 100d, now)
		Thread.sleep(1000)
		println new JSON(timeSeriesService.getMetrics(new Date(0), new Date(System.currentTimeMillis() + 180000l), 'testSaveMetrics', 'poop')).toString(true)
	}


	void testSaveMetricsWithHourlyAggregate() {
		flush('testSaveMetricsWithHourlyAggregate')
		def now = getTestDate()
		println now
		grailsApplication.config.grails.plugins.timeseries.poop.resolution = AbstractTimeSeriesProvider.ONE_SECOND
		grailsApplication.config.grails.plugins.timeseries.poop.aggregates = ['1h':'1d']
		timeSeriesService.saveMetric('testSaveMetrics', 'poop', 100d, now)
		Thread.sleep(1000)
		println new JSON(timeSeriesService.getMetricAggregates('1h',new Date(0), new Date(System.currentTimeMillis() + 180000l), 'testSaveMetrics', 'poop')).toString(true)
	}


	void testSaveMetricsOverwrite() {
		flush('testSaveMetricsOverwrite')
		def now = getTestDate()
		println now
		grailsApplication.config.grails.plugins.timeseries.poop.resolution = AbstractTimeSeriesProvider.ONE_SECOND
		timeSeriesService.saveMetric('testSaveMetricsOverwrite', 'poop', 100d, now)
		timeSeriesService.saveMetric('testSaveMetricsOverwrite', 'poop', 500d, now)
		Thread.sleep(1000)
		println new JSON(timeSeriesService.getMetrics(new Date(0), new Date(System.currentTimeMillis() + 180000l), 'testSaveMetricsOverwrite')).toString(true)
	}


	void testSaveMetricsIrregular() {
		flush('testSaveMetricsIrregular')
		def now = getTestDate()
		[11,17,5,7].each {
//			println now
			grailsApplication.config.grails.plugins.timeseries.poop.resolution = AbstractTimeSeriesProvider.ONE_SECOND
			timeSeriesService.saveMetric('testSaveMetricsIrregular', 'poop', it, now)
			now = new Date(now.time + (it*1000))
		}
		Thread.sleep(1000)
		println new JSON(timeSeriesService.getMetrics(new Date(0), new Date(System.currentTimeMillis() + 180000l), 'testSaveMetricsIrregular')).toString(true)
	}

	void testSaveMetricsRegular() {
		flush('testSaveMetricsRegular')
		def now = getTestDate()
		(1..15).each {
			//println now
			grailsApplication.config.grails.plugins.timeseries.poop.resolution = AbstractTimeSeriesProvider.ONE_SECOND
			timeSeriesService.saveMetric('testSaveMetricsRegular', 'poop', it, now)
			now = new Date(now.time + 1000)
		}
		Thread.sleep(1000)
		println new JSON(timeSeriesService.getMetrics(new Date(0), new Date(System.currentTimeMillis() + 180000l), 'testSaveMetricsRegular')).toString(true)
	}
	void testSaveMetricsRegularWithAggregates() {
		flush('testSaveMetricsRegularWithAggregates')
		grailsApplication.config.grails.plugins.timeseries.met1.aggregates = ['1m':'7d']
		def now = getTestDate()
		(1..30).each {
			//println now
			grailsApplication.config.grails.plugins.timeseries.met1.resolution = AbstractTimeSeriesProvider.ONE_SECOND
			timeSeriesService.saveMetric('testSaveMetricsRegularWithAggregates', 'met1', it, now)
			now = new Date(now.time + 1000)
		}
		Thread.sleep(1000)
		println new JSON(timeSeriesService.getMetricAggregates('1m',new Date(0), new Date(System.currentTimeMillis() + 180000l), 'testSaveMetricsRegularWithAggregates', 'met1')).toString(true)
	}


	void testSaveMetricsRegularWithGet() {
		flush('testSaveMetricsRegularWithAggregates')
		def now = getTestDate()
		(1..35).each {
			//println now
			grailsApplication.config.grails.plugins.timeseries.met1.resolution = AbstractTimeSeriesProvider.ONE_SECOND
			grailsApplication.config.grails.plugins.timeseries.met2.resolution = AbstractTimeSeriesProvider.ONE_SECOND
			timeSeriesService.saveMetrics('testSaveMetricsRegularWithGet', ['met1':it, 'met2':(121-it)], now)
			now = new Date(now.time + 1000)
		}
		Thread.sleep(1000)
		println new JSON(timeSeriesService.getMetrics(new Date(0), new Date(System.currentTimeMillis() + 180000l), 'testSaveMetricsRegularWithGet')).toString(true)
	}

	void testSaveMetricsRegularWithAggregatesWithGet() {
		flush('testSaveMetricsRegularWithAggregatesWithGet')
		grailsApplication.config.grails.plugins.timeseries.met1.aggregates = ['1m':'7d']
		grailsApplication.config.grails.plugins.timeseries.met2.aggregates = ['1m':'7d']
		grailsApplication.config.grails.plugins.timeseries.met1.resolution = AbstractTimeSeriesProvider.ONE_SECOND
		grailsApplication.config.grails.plugins.timeseries.met2.resolution = AbstractTimeSeriesProvider.ONE_SECOND
		def now = getTestDate()
		(1..35).each {
			//println now
			timeSeriesService.saveMetrics('testSaveMetricsRegularWithAggregatesWithGet', ['met1':it, 'met2':(121-it)], now)
			timeSeriesService.saveMetrics('testSaveMetricsRegularWithAggregatesWithGet2', ['met1':it*1.23, 'met2':(121-it)*1.23], now)
			now = new Date(now.time + 1000)
		}
		Thread.sleep(1000)
		println new JSON(timeSeriesService.getMetricAggregates('1m',new Date(0), new Date(System.currentTimeMillis() + 180000l),'testSaveMetricsRegularWithAggregatesWithGet')).toString(true)
	}
/*
	void testManageStorage() {
		flush()
		grailsApplication.config.grails.plugins.timeseries.met1.resolution = AbstractTimeSeriesProvider.ONE_HOUR
		grailsApplication.config.grails.plugins.timeseries.met1.expiration = '2d'
		grailsApplication.config.grails.plugins.timeseries.met1.aggregates = ['1d':'2d']
		def now = new Date(),
			old = now - 5,
			it = 1
		while (true) {
//			println 'Saving '+old
			timeSeriesService.saveMetric('testManageStorage', 'met1', it, old)
			old = new Date(old.time + 3600000l)
			if (old > now) break
			it++
		}
		Thread.sleep(500)
		println new JSON(timeSeriesService.getMetrics(new Date(0), new Date(System.currentTimeMillis() + 180000l))).toString(true)
	}
*/
}

