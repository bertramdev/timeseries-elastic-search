package grails.plugins.timeseries.es

import grails.plugins.timeseries.*
import grails.converters.*
import groovy.transform.PackageScope
import org.elasticsearch.search.facet.*
import org.elasticsearch.search.facet.FacetBuilders.*
import org.elasticsearch.search.facet.datehistogram.*
import org.elasticsearch.search.facet.statistical.*
import org.elasticsearch.search.facet.terms.*
import org.elasticsearch.index.query.*

class ElasticSearchTimeSeriesProvider extends AbstractTimeSeriesProvider {

	public ElasticSearchTimeSeriesProvider() {
		super()
	}

	@Override
	void manageStorage(groovy.util.ConfigObject config) {
		// select distinct metric names
	}

	@Override
	void init(groovy.util.ConfigObject config) {
	}

	@Override
	void shutDown(groovy.util.ConfigObject config) {
	}

	@Override
	void flush(groovy.util.ConfigObject config) {
		getElasticSearchHelper().withElasticSearch { client ->
			def response = client.prepareDeleteByQuery("time-series")
								.setQuery(org.elasticsearch.index.query.QueryBuilders.matchAllQuery())
								.execute()
								.actionGet()
		}
	}

	void flush(String refId) {
		getElasticSearchHelper().withElasticSearch { client ->
			def response = client.prepareDeleteByQuery("time-series")
								.setQuery(org.elasticsearch.index.query.QueryBuilders.matchQuery('refiId', refId))
								.execute()
								.actionGet()
		}
	}

	@Override
	String getName() {
		return 'es'
	}

	@Override
	String toString() {
		super.toString()
	}

	static METRIC_MAPPING = '''
{
	"metric" : {
		"properties" : {
			"_ttl" : { "enabled" : true },
			"refId" : {"type" : "string", "store": true, "index":"not_analyzed"},
			"metric" : {"type" : "string", "store": true, "index":"not_analyzed"},
			"value" : {"type" : "double", "index":"not_analyzed"},
			"start" : {"type" : "date"},
			"end" : {"type" : "date"},
			"_timestamp" : {
	            "enabled" : true
        	}
		}
	}
}
'''
	def addMetricMappingTemplate() {
		getElasticSearchHelper().withElasticSearch { client ->
		  client.admin()
				.indices()
				.preparePutTemplate('metric_template')
				.setTemplate('*')
				.addMapping('metric',METRIC_MAPPING)
				.execute()
		}
	}

	private getElasticSearchHelper() {
		//grailsApplication.mainContext['elasticSearchHelper']
		grails.util.Holders.grailsApplication.mainContext['elasticSearchHelper']
	}

	private searchType(nm) {
		return this.class.classLoader.loadClass('org.elasticsearch.action.search.SearchType').valueOf(nm)
	}

	private sortOrder(dir) {
		return this.class.classLoader.loadClass('org.elasticsearch.search.sort.SortOrder').valueOf(dir)
	}

	private builder(nm, Object[] args = null) {
		if (args)
			return this.class.classLoader.loadClass('org.elasticsearch.index.query.'+nm + 'Builder').newInstance(args)
		else
			return this.class.classLoader.loadClass('org.elasticsearch.index.query.'+nm + 'Builder').newInstance()
	}

	def getIndexName(end) {
		return 'time-series'//+end.format('yyyyMMdd')
	}


	@Override
	void saveMetrics(String referenceId, Map<String, Double> metrics, Date timestamp, groovy.util.ConfigObject config) {
		def startAndInterval,
			aggregates
		metrics.each {k, v->
			startAndInterval = getMetricStartAndInterval(k, timestamp, config)
			def prevValue = 0d,
				start = new Date(startAndInterval.start.time+(Long)(startAndInterval.intervalSecs * startAndInterval.interval * 1000)),
				id = referenceId+':'+k+':'+start.time,
				exp = System.currentTimeMillis() + getMillisecondExpirations(k, config),
				rec = [ _ttl:exp, _timestamp: start, refId: referenceId, metric:k, value:v, start:start, end: new Date(start.time +(Long)(startAndInterval.intervalSecs*1000) )]

			runAsync {
				//println "Async Insert"
				try {
					getElasticSearchHelper().withElasticSearch { client ->
					  client.prepareIndex('time-series', "metric",id)
							  .setSource(rec)
							  .execute()
							  .actionGet()
					}
				} catch(ex) {
					ex.printStackTrace()
					log.error("Error in ElasticSearchTimeSeriesProvider#saveMetrics while Async saving metric", ex)
				}
			}
		}

	}


	@Override
	void bulkSaveMetrics(String referenceId, List<Map<Date, Map<String, Double>>> metricsByTime, groovy.util.ConfigObject config) {
		runAsync {
			//println "Async Insert"
			try {
				getElasticSearchHelper().withElasticSearch { client ->
					def bulkRequest = client.prepareBulk()
					metricsByTime.each {timestamp, metrics->
						def startAndInterval
						metrics.each {k, v->
							startAndInterval = getMetricStartAndInterval(k, timestamp, config)
							def prevValue = 0d,
								start = new Date(startAndInterval.start.time+(Long)(startAndInterval.intervalSecs * startAndInterval.interval * 1000)),
								id = referenceId+':'+k+':'+start.time,
								exp = System.currentTimeMillis() + getMillisecondExpirations(k, config),
								rec = [ _ttl: exp,refId: referenceId, metric:k, value:v, start:start, end: new Date(start.time +(Long)(startAndInterval.intervalSecs*1000) )],
								indexName = getIndexName(rec.end)
							bulkRequest.add(client.prepareIndex(indexName, "metric",id).setSource(rec))

						}
					}
					bulkRequest.execute()
				}
			} catch(ex) {
				log.error("Error in ElasticSearchTimeSeriesProvider#bulkSaveMetrics while Async saving metric", ex)
			}
		}
	}

	@Override
	Map getMetrics(Date start, Date end, String referenceIdQuery, String metricNameQuery, Map<String, Object> options, groovy.util.ConfigObject config) {
		def rtn = [:],
			grandTotal, 
			b
		getElasticSearchHelper().withElasticSearch { client ->
			def queries = [],
			    sq = client.prepareSearch('time-series')
						.setSearchType(searchType('QUERY_AND_FETCH'))//.setOperationThreading('no_threads').setExplain(true)
						.setTypes("metric")
						.setFrom(0).setSize(10000)

			queries << builder('RangeQuery', ["start"] as Object[]).from(start).to(end)
			if (referenceIdQuery) {
				b = builder('QueryStringQuery', ['refId:'+referenceIdQuery] as Object[]) 
				b.fuzziness = org.elasticsearch.common.unit.Fuzziness.ZERO
				queries << b
			}
			if (metricNameQuery) {
				b = builder('QueryStringQuery', ['metric:'+metricNameQuery] as Object[]) 
				b.fuzziness = org.elasticsearch.common.unit.Fuzziness.ZERO
				queries << b
			}
			sq.addSort('start',  sortOrder('ASC') )
			if (queries) {
				def bq = builder('BoolQuery')
				queries.each {query->
					bq.must(query)
				}
				sq.setQuery(bq)

			}
			def response = sq.execute().actionGet()
			def hits  = response.hits
			grandTotal = hits.totalHits
			hits.hits()?.each{ hit->
				def rec = hit.sourceAsMap()
				if (rec.start instanceof String) {
					//weird inability to get date type
					rec.start = Date.parse('yyyy-MM-dd\'T\'HH:mm:ss.SSSZ', rec.start.replaceAll('Z','-0000'))
				}
				rtn[rec.refId] = rtn[rec.refId] ?: [:]
				rtn[rec.refId][rec.metric] = rtn[rec.refId][rec.metric] ?: []
				rtn[rec.refId][rec.metric] << [timestamp:rec.start, value: rec.value]
			}
		}
		def items =[]
//		println new JSON(rtn).toString(true)
		rtn.each {k, v->
			def tmp = [referenceId: k, series:[]]
			//println k+':'+v
			v.each {m, vals->
				tmp.series << [name:m, values:vals]
			}
			items << tmp
		}
		[start:start, grandTotal:grandTotal, end:end, items:items]
	}

	@Override
	Map getMetricAggregates(String resolution, Date start, Date end, String referenceIdQuery, String metricNameQuery, Map<String, Object> options, groovy.util.ConfigObject config) {
		def rtn = [:],
			refId,
			metricName = metricNameQuery,
			response

		getElasticSearchHelper().withElasticSearch { client ->
			// i need to find matching ref ids and matching metric names
			def queries = [],
			    sq = client.prepareSearch('time-series').setSearchType(searchType('COUNT')).setTypes("metric"),
			    b
			queries << builder('RangeQuery', ["start"] as Object[]).from(start).to(end)
			if (referenceIdQuery) {
				b = builder('QueryStringQuery', ['refId:'+referenceIdQuery] as Object[]) 
				b.fuzziness = org.elasticsearch.common.unit.Fuzziness.ZERO
				queries << b
			}
			def bq = builder('BoolQuery')
			queries.each {query->
				bq.must(query)
			}
			sq.setQuery(bq)

			sq.addFacet(FacetBuilders.termsFacet("t").field("refId").size(100))
			response = sq.execute().actionGet()
	
			TermsFacet t = (TermsFacet) response.getFacets().facetsAsMap().get("t")
			// For each entry
			for (TermsFacet.Entry termEntry : t) {
			    refId = termEntry.getTerm().toString()
				queries = []
			    sq = client.prepareSearch('time-series').setSearchType(searchType('COUNT')).setTypes("metric")
				b = builder('RangeQuery', ["start"] as Object[]).from(start).to(end)
				queries << b
				b =  builder('QueryStringQuery', ['refId:'+refId] as Object[]) 
				b.fuzziness = org.elasticsearch.common.unit.Fuzziness.ZERO
				queries << b
				if (metricNameQuery) {
					b = QueryBuilders.queryString('metric:'+metricNameQuery)
					b.fuzziness = org.elasticsearch.common.unit.Fuzziness.ZERO
					queries << b
				}
				bq = builder('BoolQuery')
				queries.each {query->
					bq.must(query)
				}
				sq.setQuery(bq)
				sq.addFacet(FacetBuilders.termsFacet("t2").field("metric").size(100))
				response = sq.execute().actionGet()
	
				TermsFacet t2 = (TermsFacet) response.getFacets().facetsAsMap().get("t2");
				for (TermsFacet.Entry termEntry2 : t2) {
				    metricName = termEntry2.getTerm().toString()
					queries = []
					sq = client.prepareSearch('time-series').setSearchType(searchType('COUNT')).setTypes("metric")

					b = builder('RangeQuery', ["start"] as Object[]).from(start).to(end)
					queries << b
					b = builder('QueryStringQuery', ['refId:'+refId] as Object[]) 
					b.fuzziness = org.elasticsearch.common.unit.Fuzziness.ZERO
					queries << b
					b = builder('QueryStringQuery', ['metric:'+metricName] as Object[]) 
					b.fuzziness = org.elasticsearch.common.unit.Fuzziness.ZERO
					queries << b
					bq = builder('BoolQuery')
					queries.each {query->
						bq.must(query)
					}
					sq.setQuery(bq)

					sq.addFacet(FacetBuilders.dateHistogramFacet("f").field("start").interval(resolution).valueField('value'))

					response = sq.execute().actionGet()

					DateHistogramFacet f = (DateHistogramFacet) response.getFacets().facetsAsMap().get("f")

					// For each entry
					for (DateHistogramFacet.Entry entry : f) {
						def rec = [start:new Date(entry.getTime()), 
						           sum:entry.getTotal(), 
						           count: entry.getCount(), 
						           max:entry.getMax(), 
						           min:entry.getMin(), 
						           average:entry.getMean() ]
						rtn[refId] = rtn[refId] ?: [:]
						rtn[refId][metricName] = rtn[refId][metricName] ?: []
						rtn[refId][metricName] << rec
					}
				}
			}
		}
		def items =[]
//		println new JSON(rtn).toString(true)
		rtn.each {k, v->
			def tmp = [referenceId: k, series:[]]
			v.each {m, vals->
				tmp.series << [name:m, values:vals]
			}
			items << tmp
		}
		[start:start, end:end, items:items]
	}


}