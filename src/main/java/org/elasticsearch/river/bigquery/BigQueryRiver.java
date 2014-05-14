package org.elasticsearch.river.bigquery;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.InvalidParameterException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.RhinoException;
import org.mozilla.javascript.ScriptableObject;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.SecurityUtils;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;

/**
 * The class that performs the actual import.
 * 
 * @author Ravi Gairola (mallox@pyxzl.net)
 */
public class BigQueryRiver extends AbstractRiverComponent implements River {
	private final Client	esClient;
	private volatile Thread	thread;
	private boolean			stopThread;

	private final String	project;
	private final String	account;
	private final String	keyFile;

	private final String	query;
	private final String	uniqueIdField;
	private final long		interval;
	public final String		typeScript;
	public final String		indexScript;
	private final String	mappingScript;
	private final boolean	create;

	@Inject
	public BigQueryRiver(final RiverName riverName, final RiverSettings settings, final Client esClient) {
		super(riverName, settings);
		this.esClient = esClient;
		logger.info("Creating BigQuery Stream River");
		indexScript = readConfig("index", riverName.name());
		typeScript = readConfig("type", "import");
		project = readConfig("project");
		keyFile = readConfig("keyFile");
		account = readConfig("account");
		query = readConfig("query");
		mappingScript = readConfig("mapping", null);
		create = Boolean.valueOf(readConfig("create", null));
		uniqueIdField = readConfig("uniqueIdField", null);
		interval = Long.parseLong(readConfig("interval", "600000"));
	}

	private String readConfig(final String config) {
		final String result = readConfig(config, null);
		if (result == null) {
			logger.error("Unable to read required config {}. Aborting!", config);
			throw new InvalidParameterException("Unable to read required config " + config);
		}
		return result;
	}

	@SuppressWarnings({ "unchecked" })
	private String readConfig(final String config, final String defaultValue) {
		if (settings.settings().containsKey("bigquery")) {
			Map<String, Object> bqSettings = (Map<String, Object>) settings.settings().get("bigquery");
			return XContentMapValues.nodeStringValue(bqSettings.get(config), defaultValue);
		}
		return defaultValue;
	}

	@Override
	public void start() {
		logger.info("starting bigquery stream");

		if (thread == null) {
			try {
				thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "bigquery_slurper").newThread(new Parser());
				thread.start();
			} catch (GeneralSecurityException | IOException e) {
				logger.error("Unable to create Parser Thread", e);
			}
		}
	}

	@Override
	public void close() {
		logger.info("Closing BigQuery river");
		stopThread = true;
		thread = null;
	}

	/**
	 * Parser that asynchronously writes the data from BigQuery into ElasticSearch.
	 * 
	 * @author Ravi Gairola (mallox@pyxzl.net)
	 */
	private final class Parser extends Thread {
		private final Bigquery	client;
		private List<String>	columns;
		private BigInteger		fetchedRows;
		private String			index;
		private String			type;

		/**
		 * Initialize the parser by setting up the BigQuery client ready to take take commands.
		 * 
		 * @throws GeneralSecurityException
		 * @throws IOException
		 */
		private Parser() throws GeneralSecurityException, IOException {
			final HttpTransport http = GoogleNetHttpTransport.newTrustedTransport();
			final JsonFactory jsonFactory = new JacksonFactory();
			final KeyStore keyStore = SecurityUtils.getPkcs12KeyStore();
			final InputStream key = getClass().getClassLoader().getResourceAsStream(keyFile);
			final PrivateKey p12 = SecurityUtils.loadPrivateKeyFromKeyStore(keyStore, key, "notasecret", "privatekey", "notasecret");

			final GoogleCredential.Builder credential = new GoogleCredential.Builder();
			credential.setTransport(http);
			credential.setJsonFactory(jsonFactory);
			credential.setServiceAccountId(account);
			credential.setServiceAccountScopes(Arrays.asList(BigqueryScopes.BIGQUERY));
			credential.setServiceAccountPrivateKey(p12);

			final Bigquery.Builder bq = new Bigquery.Builder(http, jsonFactory, credential.build());
			bq.setApplicationName(project);
			client = bq.build();
		}

		/**
		 * Do the thread management of running in regular intervals.
		 */
		@Override
		public void run() {
			logger.info("BigQuery Import Thread has started");
			long lastRun = 0;
			while (!stopThread) {
				final long now = System.currentTimeMillis();
				if (lastRun + interval < now) {
					lastRun = now;
					try {
						index = evalJavaScript(indexScript);
						type = evalJavaScript(typeScript);
						if (!createMapping()) {
							logger.error("failed to create datasets [{}], disabling river", index);
							return;
						}
						executeJob(null);
					} catch (ElasticsearchException | IOException | InterruptedException e) {
						logger.error("Parser was unable to run properly", e);
					}
					if (interval <= 0) {
						logger.info("Stopping BigQuery Import Thread because no interval for repeated fetches has been configured");
						break;
					}
					if (!stopThread) {
						logger.info("BigQuery Import Thread is waiting for {} Seconds until the next run", interval / 1000);
					}
				}
				try {
					sleep(250);
				} catch (InterruptedException e) {
					logger.trace("Thread sleep cycle has been interrupted", e);
				}
			}
			logger.info("BigQuery Import Thread has finished");
		}

		private boolean createMapping() {
			boolean created = false;
			final String mapping = mappingScript != null ? evalJavaScript(mappingScript) : "{\"" + type
					+ "\":{\"_timestamp\":{\"enabled\":true}}}";

			try {
				logger.debug("Creating mapping");
				esClient.admin().indices().prepareCreate(index).addMapping(type, mapping).execute().actionGet();
				created = true;
				logger.info("Created mapping for index {} with type {}", index, type);
			} catch (Exception e) {
				if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
					logger.debug("Not creating Index {} as it already exists", index);
					created = true;
				}
				else if (ExceptionsHelper.unwrapCause(e) instanceof ElasticsearchException) {
					logger.debug("Mapping {}.{} already exists and will not be created", index, type);
					created = true;
				}
			}

			if (!created) {
				try {
					logger.debug("Creating index and mapping");
					esClient.admin()
						.indices()
						.preparePutMapping(index)
						.setType(type)
						.setSource(mapping)
						.setIgnoreConflicts(true)
						.execute()
						.actionGet();
					created = true;
					logger.info("Created index and mapping for index {} with type {}", index, type);
				} catch (Exception e) {
					if (ExceptionsHelper.unwrapCause(e) instanceof ElasticsearchException) {
						logger.debug("Mapping {}.{} already exists and will not be created", index, type);
						created = true;
					}
				}
			}

			if (created) {
				Requests.flushRequest(index).force();
			}
			return created;
		}

		/**
		 * Get the json document for this row.
		 * 
		 * @param rows
		 * @return A string array with the first value holding the json document and the second element holding the unique id
		 *         (if configured)
		 * @throws IOException
		 */
		@SuppressWarnings("unchecked")
		private String[] getJson(@Nonnull final TableRow rows) throws IOException {
			final Map<String, Object> document = new HashMap<>();
			String uniqueId = null;
			for (final Entry<String, Object> row : rows.entrySet()) {
				int columnCounter = 0;
				for (TableCell field : (ArrayList<TableCell>) row.getValue()) {
					final String key = columns.get(columnCounter++);

					if (key.equals(uniqueIdField)) {
						uniqueId = (String) field.getV();
						continue;
					}

					if (field.getV() instanceof String) {
						final String value = (String) field.getV();
						if (value.startsWith("{")) {
							final Map<String, Object> map = new HashMap<>();
							try {
								rows.getFactory().createJsonParser(value).parse(map);
								document.put(key, map);
								continue;
							} catch (Exception e) {
								logger.trace("Unable to parse json of field {}", e, key);
							}
						}

						if (value.startsWith("[")) {
							final List<String> list = new ArrayList<>();
							try {
								rows.getFactory().createJsonParser(value).parse(list);
								document.put(key, list);
								continue;
							} catch (Exception e) {
								logger.trace("Unable to parse json of field {}", e, key);
							}
						}
						document.put(key, value);
					}
				}
			}
			return new String[] { rows.getFactory().toString(document), uniqueId };
		}

		/**
		 * Runs the BigQuery Job for getting the data and waits for completion.
		 * 
		 * @return
		 * @throws IOException
		 * @throws InterruptedException
		 */
		private void executeJob(@Nullable final JobReference activeJob) throws IOException, InterruptedException {
			logger.debug("Running Job to fetch data from BigQuery");
			JobReference jobReference;
			if (activeJob == null) {
				fetchedRows = new BigInteger("0");
				final Job job = new Job();
				final JobConfiguration config = new JobConfiguration();
				final JobConfigurationQuery queryConfig = new JobConfigurationQuery();

				job.setConfiguration(config.setQuery(queryConfig.setQuery(evalJavaScript(query))));

				final Insert insert = client.jobs().insert(project, job).setProjectId(project);
				jobReference = insert.execute().getJobReference();
			}
			else {
				jobReference = activeJob;
			}

			while (!stopThread) {
				final Job pollJob = client.jobs().get(project, jobReference.getJobId()).execute();

				if (pollJob.getStatus().getState().equals("DONE")) {
					logger.debug("BigQuery Job has finished, fetching results");

					if (pollJob.getStatus().getErrorResult() != null) {
						logger.info("BigQuery reported an error for the query: {}", pollJob.getStatus().getErrorResult().getMessage());
						return;
					}

					final GetQueryResultsResponse queryResult = client.jobs()
						.getQueryResults(project, pollJob.getJobReference().getJobId())
						.execute();

					if (queryResult.getTotalRows().equals(BigInteger.ZERO)) {
						logger.info("Got 0 results from BigQuery - not gonna do anything");
						return;
					}

					logger.trace("Getting column names from BigQuery to be used for building json structure");
					columns = new ArrayList<>();
					for (TableFieldSchema fieldSchema : queryResult.getSchema().getFields()) {
						columns.add(fieldSchema.getName());
					}
					fetchedRows = fetchedRows.add(new BigInteger("" + queryResult.getRows().size()));

					parse(queryResult.getRows());

					if (fetchedRows.compareTo(queryResult.getTotalRows()) < 0) {
						logger.debug("Continuing BigQuery job as not all rows could be fetched on the first request");
						executeJob(jobReference);
					}
					return;
				}
				try {
					logger.trace("Waiting for BigQuery job to be done (state is {})", pollJob.getStatus().getState());
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					logger.trace("Unable to put thread to sleep", e);
				}
			}
		}

		/**
		 * Mostly just write the data you got from BigQuery into ElasticSearch.
		 * 
		 * @throws ElasticsearchException
		 * @throws IOException
		 * @throws InterruptedException
		 */
		private void parse(@Nonnull final List<TableRow> rows) throws ElasticsearchException, IOException, InterruptedException {
			final int size = rows.size();
			int count = 0;
			final String timestamp = String.valueOf(System.currentTimeMillis());
			int progress = 0;
			logger.info("Got {} results from BigQuery database", size);

			while (!stopThread && !rows.isEmpty()) {
				final TableRow row = rows.remove(0);
				final String[] jsonResult = getJson(row);
				final String source = jsonResult[0];
				final IndexRequestBuilder builder = esClient.prepareIndex(index, type);
				if (jsonResult[1] != null) {
					builder.setId(jsonResult[1]);
				}
				try {
					builder.setOpType(OpType.CREATE)
						.setReplicationType(ReplicationType.ASYNC)
						.setOperationThreaded(true)
						.setTimestamp(timestamp)
						.setSource(source)
						.setCreate(create)
						.execute()
						.actionGet();
					count++;
				} catch (DocumentAlreadyExistsException daee) {
					logger.trace("Document already exists, create flag is {}", daee, create);
				}
				if (++progress % 100 == 0) {
					logger.debug("Processed {} entries ({} percent done)", progress, Math.round((float) progress / (float) size * 100f));
				}
			}
			logger.info("Imported {} entries into ElasticSeach from BigQuery (some duplicates may have been skipped)", count);
			return;
		}

		private String evalJavaScript(@Nonnull final String script) {
			try {
				final Context context = Context.enter();
				final ScriptableObject scope = context.initStandardObjects();
				final Object result = context.evaluateString(scope, script, "query", 1, null);
				final String evaluatedScript = Context.toString(result);
				Context.exit();
				logger.debug("String was javascript and has been evaluated to: {}", evaluatedScript);
				return evaluatedScript;
			} catch (RhinoException re) {
				return script;
			}
		}
	}
}
