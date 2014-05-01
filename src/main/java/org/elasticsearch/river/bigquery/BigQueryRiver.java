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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.mozilla.javascript.Context;
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
	public final String		type;
	public final String		index;

	@Inject
	public BigQueryRiver(final RiverName riverName, final RiverSettings settings, final Client esClient) {
		super(riverName, settings);
		this.esClient = esClient;
		logger.info("Creating BigQuery Stream River");
		index = readConfig("index", riverName.name());
		type = readConfig("type", "import");
		project = readConfig("project");
		keyFile = readConfig("keyFile");
		account = readConfig("account");
		query = readConfig("query");
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
		try {
			esClient.admin()
				.indices()
				.prepareCreate(index)
				.addMapping(type, "{\"" + type + "\":{\"_timestamp\":{\"enabled\":true}}}")
				.execute()
				.actionGet();
			logger.info("Created Index {} with _timestamp mapping for {}", index, type);
		} catch (Exception e) {
			if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
				logger.debug("Not creating Index {} as it already exists", index);
			}
			else if (ExceptionsHelper.unwrapCause(e) instanceof ElasticsearchException) {
				logger.debug("Mapping {}.{} already exists and will not be created", index, type);
			}
			else {
				logger.warn("failed to create datasets [{}], disabling river...", e, index);
				return;
			}
		}

		try {
			esClient.admin()
				.indices()
				.preparePutMapping(index)
				.setType(type)
				.setSource("{\"" + type + "\":{\"_timestamp\":{\"enabled\":true}}}")
				.setIgnoreConflicts(true)
				.execute()
				.actionGet();
		} catch (ElasticsearchException e) {
			logger.debug("Mapping already exists for datasets {} and tables {}", index, type);
		}

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
						executeJob(null);
					} catch (ElasticsearchException | IOException | InterruptedException e) {
						logger.error("Parser was unable to run properl", e);
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

		@SuppressWarnings("unchecked")
		private String getJson(@Nonnull final TableRow rows) throws IOException {
			final Map<String, Object> document = new HashMap<>();
			for (final Entry<String, Object> row : rows.entrySet()) {
				int columnCounter = 0;
				for (TableCell field : (ArrayList<TableCell>) row.getValue()) {
					final String key = columns.get(columnCounter++);

					if (field.getV() instanceof String) {
						final String value = (String) field.getV();
						if (value.startsWith("{")) {
							final Map<String, Object> map = new HashMap<>();
							try {
								rows.getFactory().createJsonParser(value).parse(map);
								document.put(key, map);
								continue;
							} catch (Exception e) {
								logger.debug("Unable to parse json of field {}", e, key);
							}
						}

						if (value.startsWith("[")) {
							final List<String> list = new ArrayList<>();
							try {
								rows.getFactory().createJsonParser(value).parse(list);
								document.put(key, list);
								continue;
							} catch (Exception e) {
								logger.debug("Unable to parse json of field {}", e, key);
							}
						}
						document.put(key, value);
					}
				}
			}
			return rows.getFactory().toString(document);
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

				job.setConfiguration(config.setQuery(queryConfig.setQuery(evalQuery(query))));

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
			final String timestamp = String.valueOf((int) (System.currentTimeMillis() / 1000));
			int progress = 0;
			logger.info("Got {} results from BigQuery database", size);

			while (!stopThread && !rows.isEmpty()) {
				final TableRow row = rows.remove(0);
				final String source = getJson(row);
				final IndexRequestBuilder builder = esClient.prepareIndex(index, type);
				if (uniqueIdField != null) {
					if (!row.containsKey(uniqueIdField)) {
						logger.error("A returned result didn't contain the configured unique id \"{}\"", uniqueIdField);
					}
					else {
						builder.setId(String.valueOf(row.get(uniqueIdField)));
					}
				}
				builder.setOpType(OpType.CREATE)
					.setReplicationType(ReplicationType.ASYNC)
					.setOperationThreaded(true)
					.setTimestamp(timestamp)
					.setSource(source)
					.execute()
					.actionGet();
				if (++progress % 100 == 0) {
					logger.debug("Processed {} entries ({} percent done)", progress, Math.round((float) progress / (float) size * 100f));
				}
			}
			logger.info("Imported {} entries into ElasticSeach from BigQuery!", size);
			return;
		}

		private String evalQuery(@Nonnull final String query) {
			if (!query.trim().toLowerCase().startsWith("select")) {
				final Context context = Context.enter();
				final ScriptableObject scope = context.initStandardObjects();
				final Object result = context.evaluateString(scope, query, "query", 1, null);
				final String evaluatedQuery = Context.toString(result);
				logger.debug("Query was javascript and has been evaluated to: {}", evaluatedQuery);
				return evaluatedQuery;
			}
			return query;
		}
	}
}
