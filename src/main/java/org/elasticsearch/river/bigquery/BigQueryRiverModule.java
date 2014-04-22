package org.elasticsearch.river.bigquery;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

/**
 * The Module that performs the actual binding of the BigQuery module.
 * 
 * @author Ravi Gairola (ravig@motorola.com)
 */
public class BigQueryRiverModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(River.class).to(BigQueryRiver.class).asEagerSingleton();
	}
}
