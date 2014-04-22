package org.elasticsearch.plugin.river.bigquery;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.bigquery.BigQueryRiverModule;

/**
 * Class for registering the BigQuery river plugin.
 * 
 * @author Ravi Gairola (mallox@pyxzl.net)
 */
public class BigQueryRiverPlugin extends AbstractPlugin {

	@Inject
	public BigQueryRiverPlugin() {}

	@Override
	public String name() {
		return "river-bigquery";
	}

	@Override
	public String description() {
		return "BigQuery River Plugin";
	}

	/**
	 * Makes the module available to be loaded by ES.
	 * 
	 * @param module
	 */
	public void onModule(final RiversModule module) {
		module.registerRiver("river-bigquery", BigQueryRiverModule.class);
	}
}
