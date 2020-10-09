package job.opener.sdk;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.functions.LocalRunner;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Saves (to database all the details of) a new (instant) job request from a
 * consumer. Then finds (5) online and in-proximity (within 10000 meters radius)
 * workers who can perform the required tasks (marked required by the consumer).
 * Then writes job offer to workers' topics and marks their status as ON-OFFER.
 * (2 seconds) Later monitors the sent job offer for (read) receipt, marking
 * OFFLINE the workers who (were sent the offer but) did not respond with a
 * (read) receipt. (After further 9 seconds) checks for acceptance by any of the
 * workers (who received the job offer), marking as ONLINE again the workers who
 * (received the offer but) did not accept. Responds to the consumer accordingly
 * with success (containing job and worker IDs) or with failure (containing
 * reason).
 *
 * @author Hares Mahmood
 * @author Khubaib Shabbir
 */
public class JobOpener implements Function<String, String> {
	// Database details
	static final String DB_URL = "jdbc:mysql://localhost/hyperworldly?serverTimezone=UTC";
	static final String USERNAME = "hyperworldly";
	static final String PASSWORD = "bPpXfhoT2ZkzQOFP";
	// The Logger
	Logger LOG = null;

	/**
	 * Gets a database connection.
	 * 
	 * @param origin
	 * @return
	 * @throws SQLException
	 */
	private Connection getDatabaseConnection(String origin) throws SQLException {
		// Open SQL connection
		Connection sqlConnection = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
		LOG.info("{} CONNECTED TO DATABASE.", origin);
		return sqlConnection;
	}

	/**
	 * Extracts the query from a prepared statement and logs it.
	 * 
	 * @param preparedStatement
	 */
	private void logPreparedStatement(PreparedStatement preparedStatement) {
		// Get SQL query from the statement
		String preparedStatementAsString = preparedStatement.toString();
		String preparedSqlQuery = preparedStatementAsString.substring(preparedStatementAsString.indexOf(":") + 2,
				preparedStatementAsString.length());
		LOG.info("EXECUTING {}", preparedSqlQuery);
	}

	/**
	 * Prepares a comma-separated string, from an array of integers.
	 * 
	 * @param intArray
	 * @return
	 */
	private String commaSeparatedStringfromIntArray(int[] intArray) {
		String commaSeparatedIntegersFromArray = "";
		if (intArray.length > 0) {
			StringBuilder sb = new StringBuilder();
			for (int integer : intArray) {
				sb.append(integer).append(",");
			}
			commaSeparatedIntegersFromArray = sb.deleteCharAt(sb.length() - 1).toString();
		}
		LOG.info("commaSeparatedIntegersFromArray ARE: {}", commaSeparatedIntegersFromArray);
		return commaSeparatedIntegersFromArray;
	}

	/**
	 * Saves new job to the `jobs` table in the database.
	 * 
	 * @param sqlConnection
	 * @param serviceCategoryId
	 * @param serviceLevelId
	 * @param consumerId
	 * @param requestedOn
	 * @return
	 * @throws SQLException
	 */
	private int saveNewJob(Connection sqlConnection, String requestedOn) throws SQLException {
		String table = "jobs";
		String columns = "status, requested_on, added_on";
		String sqlStringToPrepare = "INSERT INTO " + table + " (" + columns + ") VALUES ('OPEN', ?, NOW())";
		// Prepare SQL statement
		PreparedStatement preparedStatement = sqlConnection.prepareStatement(sqlStringToPrepare,
				Statement.RETURN_GENERATED_KEYS);
		preparedStatement.setString(1, requestedOn);
		logPreparedStatement(preparedStatement);
		// Execute prepared statement
		preparedStatement.execute();
		// Get inserted id
		int jobId = 0;
		ResultSet resultSet = preparedStatement.getGeneratedKeys();
		if (resultSet.next()) {
			jobId = resultSet.getInt(1);
		}
		LOG.info("SAVED JOB UNDER ID {} INTO {} TABLE", jobId, table);
		return jobId;
	}

	/**
	 * Saves consumer ID against job ID in `job_consumer` table in database.
	 * 
	 * @param sqlConnection
	 * @param jobId
	 * @param consumerId
	 * @throws SQLException
	 */
	private void saveNewJobConsumer(Connection sqlConnection, int jobId, int consumerId) throws SQLException {
		String table = "job_consumer";
		String columns = "job_id, consumer_id, added_on";
		String sqlStringToPrepare = "INSERT INTO " + table + " (" + columns + ") VALUES (?, ?, NOW())";
		// Prepare SQL statement
		PreparedStatement preparedStatement = sqlConnection.prepareStatement(sqlStringToPrepare,
				Statement.RETURN_GENERATED_KEYS);
		preparedStatement.setInt(1, jobId);
		preparedStatement.setInt(2, consumerId);
		logPreparedStatement(preparedStatement);
		// Execute the prepared statement
		preparedStatement.execute();
		// Get the inserted id
		ResultSet resultSet = preparedStatement.getGeneratedKeys();
		int generatedId = 0;
		if (resultSet.next()) {
			generatedId = resultSet.getInt(1);
		}
		LOG.info("SAVED CONSUMER AGAINST JOB UNDER ID {} INTO {} TABLE", generatedId, table);
	}

	/**
	 * Saves expertise level ID against job ID in `job_expertise_level` table in
	 * database.
	 * 
	 * @param sqlConnection
	 * @param jobId
	 * @param expertiseLevelId
	 * @throws SQLException
	 */
	private void saveNewJobExpertiseLevel(Connection sqlConnection, int jobId, int expertiseLevelId)
			throws SQLException {
		String table = "job_expertise_level";
		String columns = "job_id, expertise_level_id, added_on";
		String sqlStringToPrepare = "INSERT INTO " + table + " (" + columns + ") VALUES (?, ?, NOW())";
		// Prepare SQL statement
		PreparedStatement preparedStatement = sqlConnection.prepareStatement(sqlStringToPrepare,
				Statement.RETURN_GENERATED_KEYS);
		preparedStatement.setInt(1, jobId);
		preparedStatement.setInt(2, expertiseLevelId);
		logPreparedStatement(preparedStatement);
		// Execute the prepared statement
		preparedStatement.execute();
		// Get the inserted id
		ResultSet resultSet = preparedStatement.getGeneratedKeys();
		int generatedId = 0;
		if (resultSet.next()) {
			generatedId = resultSet.getInt(1);
		}
		LOG.info("SAVED EXPERTISE LEVEL AGAINST JOB UNDER ID {} INTO {} TABLE", generatedId, table);
	}

	/**
	 * Saves required task IDs and respective quantities against job ID in
	 * `jobs_tasks_and_quantities` table in database.
	 * 
	 * @param processSqlConnection
	 * @param jobId
	 * @param requiredTaskIds
	 */
	private void saveNewJobTasksAndQuantities(Connection sqlConnection, int jobId, int[] requiredTaskIds,
			int[] requiredTaskQuantities) throws SQLException {
		String table = "jobs_tasks_and_quantities";
		String columns = "job_id, service_category_task_id, quantity, added_on";
		String sqlStringToPrepare = "INSERT INTO " + table + " (" + columns + ") VALUES (?, ?, ?, NOW())";
		for (int i = 0; i < requiredTaskIds.length; i++) {
			// Prepare SQL statement
			PreparedStatement preparedStatement = sqlConnection.prepareStatement(sqlStringToPrepare,
					Statement.RETURN_GENERATED_KEYS);
			preparedStatement.setInt(1, jobId);
			preparedStatement.setInt(2, requiredTaskIds[i]);
			preparedStatement.setInt(3, requiredTaskQuantities[i]);
			logPreparedStatement(preparedStatement);
			// Execute the prepared statement
			preparedStatement.execute();
			// Get the inserted id
			ResultSet resultSet = preparedStatement.getGeneratedKeys();
			int generatedId = 0;
			if (resultSet.next()) {
				generatedId = resultSet.getInt(1);
			}
			LOG.info("SAVED REQUIRED TASK AGAINST JOB UNDER ID {} INTO {} TABLE", generatedId, table);
		}
	}

	/**
	 * Saves location against job ID in `job_location` table in database.
	 * 
	 * @param sqlConnection
	 * @param jobId
	 * @param latitude
	 * @param longitude
	 * @throws SQLException
	 */
	private void saveNewJobLocation(Connection sqlConnection, int jobId, double latitude, double longitude)
			throws SQLException {
		// Prepare SQL statement
		String table = "job_location";
		String columns = "job_id, latitude, longitude, added_on";
		String sqlStringToPrepare = "INSERT INTO " + table + " (" + columns + ") VALUES (?, ?, ?, NOW())";
		PreparedStatement preparedStatement = sqlConnection.prepareStatement(sqlStringToPrepare,
				Statement.RETURN_GENERATED_KEYS);
		preparedStatement.setInt(1, jobId);
		preparedStatement.setDouble(2, latitude);
		preparedStatement.setDouble(3, longitude);
		logPreparedStatement(preparedStatement);
		// Execute the prepared statement
		preparedStatement.execute();
		// Get the inserted id
		ResultSet resultSet = preparedStatement.getGeneratedKeys();
		int generatedId = 0;
		if (resultSet.next()) {
			generatedId = resultSet.getInt(1);
		}
		LOG.info("SAVED LOCATION AGAINST JOB UNDER ID {} INTO {} TABLE", generatedId, table);
	}

	/**
	 * Finds nearest online workers for the required tasks, using Spherical Law of
	 * Cosines and job location.
	 * 
	 * @param sqlConnection
	 * @param requiredTaskIds
	 * @param expertiseLevelId
	 * @param latitude
	 * @param longitude
	 * @param radius
	 * @return
	 * @throws SQLException
	 */
	private List<Integer> findNearestOnlineWorkers(Connection sqlConnection, int[] requiredTaskIds,
			int expertiseLevelId, double latitude, double longitude, int radius) throws SQLException {
		String concatenatedRequiredTaskIds = commaSeparatedStringfromIntArray(requiredTaskIds);
		// Set volumetric mean radius of Earth in meters
		int volumetricMeanRadiusOfEarth = 6371008;
		// Prepare SQL statement (using Spherical Law of Cosines get nearest worker)
		String sqlStringToPrepare = "SELECT `worker_id`, (" + volumetricMeanRadiusOfEarth + " * ACOS(LEAST(1.0, COS("
				+ "RADIANS(" + latitude + ")) * COS(RADIANS(`latitude`)) * COS(RADIANS(`longitude`) - RADIANS("
				+ longitude + ")) + SIN(RADIANS(" + latitude + ")) * SIN(RADIANS(`latitude`))))) AS"
				+ " `distance` FROM `worker_location` WHERE `worker_id` IN (SELECT `worker_id` FROM `worker_status` "
				+ "WHERE `worker_id` IN (SELECT `worker_id` FROM `workers_tasks_and_expertise` WHERE "
				+ "`service_category_task_id` IN (" + concatenatedRequiredTaskIds + ") AND `expertise_level_id`="
				+ expertiseLevelId + " GROUP BY `worker_id` HAVING COUNT(DISTINCT `service_category_task_id`)="
				+ requiredTaskIds.length + ") AND `status`='ONLINE') ORDER BY `distance` LIMIT 5";
		PreparedStatement preparedStatement = sqlConnection.prepareStatement(sqlStringToPrepare);
		logPreparedStatement(preparedStatement);
		// Execute the prepared statement and get result
		ResultSet resultSet = preparedStatement.executeQuery();
		List<Integer> returnedIds = new ArrayList<Integer>();
		while (resultSet.next()) {
			if (Integer.valueOf(resultSet.getInt(2)) < radius) {
				returnedIds.add(Integer.valueOf(resultSet.getInt(1)));
			}
		}
		return returnedIds;
	}

	/**
	 * Finds nearest online workers for the required tasks, using Spherical Law of
	 * Cosines and job location, excluding the workers who previously received this
	 * job.
	 *
	 * @param sqlConnection
	 * @param requiredTaskIds
	 * @param expertiseLevelId
	 * @param latitude
	 * @param longitude
	 * @param radius
	 * @param jobId
	 * @return
	 * @throws SQLException
	 */
	private List<Integer> findNewNearestOnlineWorkers(Connection sqlConnection, int[] requiredTaskIds,
			int expertiseLevelId, double latitude, double longitude, int radius, int jobId) throws SQLException {
		String concatenatedRequiredTaskIds = commaSeparatedStringfromIntArray(requiredTaskIds);
		// Set volumetric mean radius of Earth in meters
		int volumetricMeanRadiusOfEarth = 6371008;
		// Prepare SQL statement (using Spherical Law of Cosines get nearest worker)
		String sqlStringToPrepare = "SELECT `worker_id`, (" + volumetricMeanRadiusOfEarth + " * ACOS(LEAST(1.0, COS("
				+ "RADIANS(" + latitude + ")) * COS(RADIANS(`latitude`)) * COS(RADIANS(`longitude`) - RADIANS("
				+ longitude + ")) + SIN(RADIANS(" + latitude + ")) * SIN(RADIANS(`latitude`))))) AS"
				+ " `distance` FROM `worker_location` WHERE `worker_id` IN (SELECT `worker_id` FROM `worker_status` "
				+ "WHERE `worker_id` IN (SELECT `worker_id` FROM `workers_tasks_and_expertise` WHERE "
				+ "`service_category_task_id` IN (" + concatenatedRequiredTaskIds + ") AND `expertise_level_id`="
				+ expertiseLevelId + " AND `worker_id` NOT IN (SELECT `worker_id` FROM `jobs_receipts` WHERE "
				+ "`job_id`=" + jobId + ") GROUP BY `worker_id` HAVING COUNT(DISTINCT `service_category_task_id`)="
				+ requiredTaskIds.length + ") AND `status`='ONLINE') ORDER BY `distance` LIMIT 5";
		PreparedStatement preparedStatement = sqlConnection.prepareStatement(sqlStringToPrepare);
		logPreparedStatement(preparedStatement);
		// Execute the prepared statement and get result
		ResultSet resultSet = preparedStatement.executeQuery();
		List<Integer> returnedIds = new ArrayList<Integer>();
		while (resultSet.next()) {
			if (Integer.valueOf(resultSet.getInt(2)) < radius) {
				returnedIds.add(Integer.valueOf(resultSet.getInt(1)));
			}
		}
		return returnedIds;
	}

	/**
	 * Saves a worker ID against a job ID in `jobs_offered_workers` table in the
	 * database.
	 * 
	 * @param sqlConnection
	 * @param jobId
	 * @param workerIds
	 * @throws SQLException
	 */
	private void saveWorkersAgainstJobOffer(Connection sqlConnection, int jobId, List<Integer> workerIds)
			throws SQLException {
		// Prepare SQL statement
		String table = "jobs_offered_workers";
		String columns = "job_id, worker_id, added_on";
		for (int workerId : workerIds) {
			String sqlStringToPrepare = "INSERT INTO " + table + " (" + columns + ") VALUES (?, ?, NOW())";
			PreparedStatement preparedStatement = sqlConnection.prepareStatement(sqlStringToPrepare);
			preparedStatement.setInt(1, jobId);
			preparedStatement.setInt(2, workerId);
			logPreparedStatement(preparedStatement);
			// Execute the prepared statement
			preparedStatement.execute();
			String sqlStringToBePrepared = "UPDATE `worker_status` SET `status`='ON-OFFER', `last_updated_on`=NOW() "
					+ "WHERE `worker_id`=?";
			PreparedStatement preparedSqlStatement = sqlConnection.prepareStatement(sqlStringToBePrepared);
			preparedSqlStatement.setInt(1, workerId);
			logPreparedStatement(preparedSqlStatement);
			// Execute the prepared statement
			preparedSqlStatement.execute();
		}
		LOG.info("SAVED JOB AND THE WORKERS IT WAS OFFERED TO, IN {} TABLE", table);
	}

	/**
	 * Prepares as a JSON object a job offer - to send to the worker's topic.
	 * 
	 * @param jobAsJsonObject
	 * @param jobId
	 * @param toProcessAgain
	 * @return
	 * @throws Exception
	 */
	private JsonObject prepareJobOfferToSend(JsonObject jobAsJsonObject, int jobId, boolean toProcessAgain)
			throws Exception {
		// Remove (any/old) jobId if present
		if (jobAsJsonObject.has("jobId")) {
			jobAsJsonObject.remove("jobId");
		}
		// Add the latest jobId and workerId
		jobAsJsonObject.addProperty("jobId", jobId);
		// Create new (offer) container JSON object
		JsonObject offerAsJson = new JsonObject();
		if (toProcessAgain) {
			offerAsJson.add("jobOpenRequest", jobAsJsonObject);
		} else {
			offerAsJson.add("instantJobOffer", jobAsJsonObject);
		}
		// Get job JSON as string
		LOG.info("JOB OFFER TO SEND {}", offerAsJson.toString());
		return offerAsJson;
	}

	/**
	 * Checks - in the `jobs_receipts` table in the database - if a job offer sent
	 * to workers was actually received by any of them.
	 * 
	 * @param sqlConnection
	 * @param jobId
	 * @return
	 * @throws SQLException
	 */
	private boolean wasJobReceivedByAnyWorker(Connection sqlConnection, int jobId) throws SQLException {
		boolean result = false;
		// Prepare SQL statement
		String sqlStringToPrepare = "SELECT `worker_id` FROM `jobs_receipts` WHERE `job_id`=?";
		PreparedStatement preparedStatement = sqlConnection.prepareStatement(sqlStringToPrepare);
		preparedStatement.setInt(1, jobId);
		logPreparedStatement(preparedStatement);
		// Execute the prepared statement and get result
		ResultSet resultSet = preparedStatement.executeQuery();
		if (resultSet.next()) {
			result = true;
		}
		// Those who did not receive the job offer and have no status changes in
		// previous 2 second, are OFFLINE
		String sqlStringToBePrepared = "";
		if (result) {
			sqlStringToBePrepared = "UPDATE `worker_status` SET `status`='OFFLINE', `last_updated_on`=NOW() WHERE "
					+ "`worker_id` IN (SELECT `worker_id` FROM `jobs_offered_workers` WHERE `worker_id` NOT IN (SELECT"
					+ " `worker_id` FROM `jobs_receipts` WHERE `job_id`=" + jobId + ")) AND `status`='ON-OFFER'"
					+ " AND `last_updated_on` > (SELECT DATE_SUB(NOW(), INTERVAL 2 SECOND))";
		} else {
			sqlStringToBePrepared = "UPDATE `worker_status` SET `status`='OFFLINE', `last_updated_on`=NOW() WHERE "
					+ "`worker_id` IN (SELECT `worker_id` FROM `jobs_offered_workers` WHERE `job_id`=" + jobId + ") "
					+ "AND `status`='ON-OFFER' AND `last_updated_on` > (SELECT DATE_SUB(NOW(), INTERVAL 2 SECOND))";
		}
		PreparedStatement preparedSqlStatement = sqlConnection.prepareStatement(sqlStringToBePrepared);
		logPreparedStatement(preparedSqlStatement);
		// Execute the prepared statement and get result
		preparedSqlStatement.execute();
		return result;
	}

	/**
	 * Checks - in the `job_worker` table in the database - if a job offer sent to a
	 * worker was actually accepted.
	 * 
	 * @param sqlConnection
	 * @param jobId
	 * @return
	 * @throws SQLException
	 */
	private boolean wasJobAcceptedByAnyWorker(Connection sqlConnection, int jobId) throws SQLException {
		boolean result = false;
		// Prepare SQL statement
		String sqlStringToPrepare = "SELECT `worker_id` FROM `job_worker` WHERE `job_id`=?";
		PreparedStatement preparedStatement = sqlConnection.prepareStatement(sqlStringToPrepare);
		preparedStatement.setInt(1, jobId);
		logPreparedStatement(preparedStatement);
		// Execute the prepared statement and get result
		ResultSet resultSet = preparedStatement.executeQuery();
		if (resultSet.next()) {
			result = true;
		}
		// Those who received the offer, but did not accept or reject it, and have no
		// status changes in previous 10 seconds, should be ONLINE to get a new offer
		String sqlStringToBePrepared = "";
		if (result) {
			sqlStringToBePrepared = "UPDATE `worker_status` SET `status`='ONLINE', `last_updated_on`=NOW() WHERE "
					+ "`worker_id` IN (SELECT `worker_id` FROM `jobs_receipts` WHERE `worker_id` NOT IN (SELECT "
					+ "`worker_id` FROM `job_worker` WHERE `job_id`=" + jobId + ")) AND `status`='ON-OFFER' AND "
					+ "`last_updated_on` > (SELECT DATE_SUB(NOW(), INTERVAL 12 SECOND))";
		} else {
			sqlStringToBePrepared = "UPDATE `worker_status` SET `status`='ONLINE', `last_updated_on`=NOW() WHERE "
					+ "`worker_id` IN (SELECT `worker_id` FROM `jobs_receipts` WHERE `job_id`=" + jobId + ") AND "
					+ "`status`='ON-OFFER' AND `last_updated_on` > (SELECT DATE_SUB(NOW(), INTERVAL 12 SECOND))";
		}
		PreparedStatement preparedSqlStatement = sqlConnection.prepareStatement(sqlStringToBePrepared);
		logPreparedStatement(preparedSqlStatement);
		// Execute the prepared statement and get result
		preparedSqlStatement.execute();
		return result;
	}

	/**
	 * Marks job status as FAILED in the `jobs` table in the database.
	 * 
	 * @param sqlConnection
	 * @param jobId
	 * @throws SQLException
	 */
	private void markJobAsFailed(Connection sqlConnection, int jobId) throws SQLException {
		// Prepare SQL statement
		String table = "jobs";
		String sqlStringToPrepare = "UPDATE " + table + " SET `status`=?, `last_updated_on`=NOW() WHERE `id`=?";
		PreparedStatement preparedStatement = sqlConnection.prepareStatement(sqlStringToPrepare);
		preparedStatement.setString(1, "FAILED");
		preparedStatement.setInt(2, jobId);
		logPreparedStatement(preparedStatement);
		// Execute the prepared statement
		preparedStatement.executeUpdate();
		LOG.info("UPDATED STATUS FOR JOB ID {} AS FAILED IN {} TABLE", jobId, table);
	}

	/**
	 * Saves the failure details of a job in the `job_failure_detail` table in the
	 * database
	 * 
	 * @param sqlConnection
	 * @param jobId
	 * @throws SQLException
	 */
	private void saveJobFailureDetail(Connection sqlConnection, int jobId, String failureReason) throws SQLException {
		// Prepare SQL statement
		String table = "job_failure_detail";
		String columns = "job_id, details, added_on";
		String sqlStringToPrepare = "INSERT INTO " + table + " (" + columns + ") VALUES (?, ?, NOW())";
		PreparedStatement preparedStatement = sqlConnection.prepareStatement(sqlStringToPrepare,
				Statement.RETURN_GENERATED_KEYS);
		preparedStatement.setInt(1, jobId);
		preparedStatement.setString(2, failureReason);
		logPreparedStatement(preparedStatement);
		// Execute the prepared statement
		preparedStatement.execute();
		// Get the inserted id
		ResultSet resultSet = preparedStatement.getGeneratedKeys();
		int generatedId = 0;
		if (resultSet.next()) {
			generatedId = resultSet.getInt(1);
		}
		LOG.info("SAVED JOB FAILURE DETAILS UNDER ID {} INTO {} TABLE", generatedId, table);
	}

	/**
	 * Gets the total elapsed time since a provided date and time string.
	 * 
	 * @return
	 * @throws ParseException
	 */
	private int getTotalElapsedTime(String requestedOn) throws ParseException {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date currentDate = new Date();
		Date requestedOnDate = dateFormat.parse(requestedOn);
		long difference = currentDate.getTime() - requestedOnDate.getTime();
		return (int) (difference / (1000));
	}

	/**
	 * Gets the ID for the worker who accepted a job ID.
	 * 
	 * @param sqlConnection
	 * @param jobId
	 * @return
	 * @throws SQLException
	 */
	private int getWorkerForJob(Connection sqlConnection, int jobId) throws SQLException {
		// Prepare SQL statement
		String sqlStringToPrepare = "SELECT `worker_id` FROM `job_worker` WHERE `job_id`=?";
		PreparedStatement preparedStatement = sqlConnection.prepareStatement(sqlStringToPrepare);
		preparedStatement.setInt(1, jobId);
		logPreparedStatement(preparedStatement);
		// Execute the prepared statement and get result
		ResultSet resultSet = preparedStatement.executeQuery();
		int workerId = 0;
		if (resultSet.next()) {
			workerId = resultSet.getInt(1);
		}
		return workerId;
	}

	/**
	 * Sends a JSON message to the specified consumer's topic.
	 * 
	 * @param jsonMessage
	 * @param consumerId
	 * @param context
	 * @throws PulsarClientException
	 */
	private void sendJsonToConsumerTopic(JsonObject jsonMessage, int consumerId, Context context)
			throws PulsarClientException {
		String consumerTopic = "consumer-" + consumerId;
		context.newOutputMessage(consumerTopic, Schema.STRING).value(jsonMessage.toString()).send();
		LOG.info("SENT JSON MESSAGE TO CONSUMER TOPIC {}", consumerTopic);
	}

	/**
	 * Sends a JSON message to the specified worker's topic.
	 * 
	 * @param jsonMessage
	 * @param workerId
	 * @param context
	 * @throws PulsarClientException
	 */
	private void sendJsonToWorkerTopic(JsonObject jsonMessage, int workerId, Context context)
			throws PulsarClientException {
		String workerTopic = "worker-" + workerId;
		context.newOutputMessage(workerTopic, Schema.STRING).value(jsonMessage.toString()).send();
		LOG.info("SENT JSON MESSAGE TO WORKER TOPIC {}", workerTopic);
	}

	/**
	 * The process method gets invoked every time a message is received on the input
	 * topic (i.e., instant-jobs-opener-input).
	 */
	@Override
	public String process(String input, Context context) {
		// Get process start nano time
		long processStartNanoseconds = System.nanoTime();
		// Set up date format
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		// Get process start date and time
		String processStartDateTime = dateFormat.format(new Date());
		// Setup Logger
		LOG = context.getLogger();
		// Get messageId (Is that the standard way to get it?)
		String message = context.getCurrentRecord().getMessage().toString();
		String messageId = message.substring(message.lastIndexOf("@") + 1, message.length() - 1);
		// Start logging
		LOG.info("STARTED PROCESSING MESSAGE ID {} ON {}.", messageId, processStartDateTime);
		// SQL variables
		Connection processSqlConnection = null;
		try {
			LOG.info("RECEIVED INPUT: {}", input.toString());
			// Load variables
			int jobId, consumerId, serviceId, expertiseLevelId;
			double latitude, longitude;
			String requestedOn = null;
			String failureReason = null;
			// Parse the input message
			LOG.info("STARTED PARSING NEW JOB REQUEST");
			JsonObject inputAsJson = new JsonParser().parse(input).getAsJsonObject();
			final JsonObject jobAsJsonObject = inputAsJson.getAsJsonObject("jobOpenRequest");
			consumerId = jobAsJsonObject.get("consumerId").getAsInt();
			serviceId = jobAsJsonObject.get("serviceId").getAsInt();
			expertiseLevelId = jobAsJsonObject.get("expertiseLevelId").getAsInt();
			requestedOn = jobAsJsonObject.get("requestedOn").getAsString();
			JsonArray location = jobAsJsonObject.getAsJsonArray("location");
			latitude = location.get(0).getAsDouble();
			longitude = location.get(1).getAsDouble();
			JsonArray requiredTaskIdsAndQuantities = jobAsJsonObject.getAsJsonArray("requiredTaskIdsAndQuantities");
			int requiredTasksSize = requiredTaskIdsAndQuantities.size();
			int[] requiredTaskIds = new int[requiredTasksSize];
			for (int i = 0; i < requiredTasksSize; i++) {
				JsonArray requiredTaskIdAndQuantity = (JsonArray) requiredTaskIdsAndQuantities.get(i);
				for (int j = 0; j < requiredTasksSize; j++) {
					requiredTaskIds[i] = requiredTaskIdAndQuantity.get(0).getAsInt();
				}
			}
			int[] requiredTaskQuantities = new int[requiredTasksSize];
			for (int i = 0; i < requiredTasksSize; i++) {
				JsonArray requiredTaskIdAndQuantity = (JsonArray) requiredTaskIdsAndQuantities.get(i);
				for (int j = 0; j < requiredTasksSize; j++) {
					requiredTaskQuantities[i] = requiredTaskIdAndQuantity.get(1).getAsInt();
				}
			}
			int radius = 10000;
			// Get SQL connection
			processSqlConnection = getDatabaseConnection("PROCESS");
			List<Integer> workerIds = new ArrayList<Integer>();
			// Check if the process() has received a new job
			if (!input.contains("jobId")) {
				LOG.info("THIS IS THE FIRST ATTEMPT ON PROCESSING MESSAGE ID {}.", messageId);
				LOG.info(
						"consumerId IS {}, serviceId IS {}, expertiseLevelId IS {}, requestedOn IS {}, latitude "
								+ "IS {}, longitude IS {}, requiredTaskIds ARE {}, requiredTaskQuantities ARE {}",
						consumerId, serviceId, expertiseLevelId, requestedOn, latitude, longitude, requiredTaskIds,
						requiredTaskQuantities);
				// Save the new job and get an id for it
				jobId = saveNewJob(processSqlConnection, requestedOn);
				// Save the job against consumer
				saveNewJobConsumer(processSqlConnection, jobId, consumerId);
				// Save the required expertiseLevel against job
				saveNewJobExpertiseLevel(processSqlConnection, jobId, expertiseLevelId);
				// Save the required tasks against job
				saveNewJobTasksAndQuantities(processSqlConnection, jobId, requiredTaskIds, requiredTaskQuantities);
				// Save the location for the job
				saveNewJobLocation(processSqlConnection, jobId, latitude, longitude);
				// Get IDs of matching workers online nearby
				workerIds = findNearestOnlineWorkers(processSqlConnection, requiredTaskIds, expertiseLevelId, latitude,
						longitude, radius);
			} else {
				LOG.info("THIS IS A REPEAT ATTEMPT ON PROCESSING MESSAGE ID {}", messageId);
				// Parse the sent offer message
				jobId = jobAsJsonObject.get("jobId").getAsInt();
				int totalElapsedTime = getTotalElapsedTime(requestedOn);
				if (totalElapsedTime <= 36) {
					LOG.info(
							"jobId IS {}, consumerId IS {}, serviceId IS {}, expertiseLevelId IS {}, "
									+ "requestedOn IS {}, latitude IS {}, longitude IS {}, requiredTaskIds ARE {}",
							jobId, consumerId, serviceId, expertiseLevelId, requestedOn, latitude, longitude,
							requiredTaskIds);
					// Get IDs of new matching workers online nearby
					workerIds = findNewNearestOnlineWorkers(processSqlConnection, requiredTaskIds, expertiseLevelId,
							latitude, longitude, radius, jobId);
				} else {
					failureReason = "36 SECONDS HAVE ELAPSED, AND NO WORKER AVAILABLE NEARBY ACCEPTED THE JOB.";
				}
			}
			// Ensure that we have some worker IDs
			if (!workerIds.isEmpty()) {
				// Save the worker IDs against job's id for future reference
				saveWorkersAgainstJobOffer(processSqlConnection, jobId, workerIds);
				// Prepare a job offer message containing the workerId and the jobId
				JsonObject jobOffer = prepareJobOfferToSend(jobAsJsonObject, jobId, false);
				// Send the job offer to each of the matching workers' topic
				for (int workerId : workerIds) {
					sendJsonToWorkerTopic(jobOffer, workerId, context);
				}
				// Build a future Runnable that checks for job receipt
				Runnable receiptRunnable = new Runnable() {
					@Override
					public void run() {
						try {
							JsonObject jobOpenRequest = prepareJobOfferToSend(jobAsJsonObject, jobId, true);
							// Get database connection
							Connection receiptRunnableSqlConnection = getDatabaseConnection("RECEIPT RUNNABLE");
							// Check if a sent job was marked as received
							boolean jobReceived = wasJobReceivedByAnyWorker(receiptRunnableSqlConnection, jobId);
							// Close database connection
							receiptRunnableSqlConnection.close();
							LOG.info("CLOSED RECEIPT RUNNABLE DATABASE CONNECTION.");
							if (!jobReceived) {
								LOG.info(
										"THE JOB HAS NOT BEEN MARKED RECEIVED BY ANY OF THE WORKERS IT WAS SENT TO. REPEATING PROCESS.");
								// Repeat the whole process with the sent offer as input
								process(jobOpenRequest.toString(), context);
							} else {
								LOG.info("THE JOB HAS BEEN MARKED RECEIVED BY ONE OF THE WORKERS IT WAS SENT TO.");
								// Build a future Runnable that checks for job acceptance
								Runnable acceptanceRunnable = new Runnable() {
									@Override
									public void run() {
										try {
											// Get database connection
											Connection acceptanceRunnableSqlConnection = getDatabaseConnection(
													"ACCEPTANCE RUNNABLE");
											// Check if a sent job was marked as accepted
											boolean jobAccepted = wasJobAcceptedByAnyWorker(
													acceptanceRunnableSqlConnection, jobId);
											int workerId = getWorkerForJob(acceptanceRunnableSqlConnection, jobId);
											// Close database connection
											acceptanceRunnableSqlConnection.close();
											LOG.info("CLOSED ACCEPTANCE RUNNABLE DATABASE CONNECTION");
											if (!jobAccepted) {
												LOG.info("THE JOB HAS NOT BEEN MARKED ACCEPTED BY ANY OF THE WORKERS IT"
														+ " WAS SENT TO. REPEATING PROCESS.");
												process(jobOpenRequest.toString(),
														context);
											} else {
												LOG.info(
														"THE JOB HAS BEEN MARKED ACCEPTED BY THE WORKER IT WAS SENT TO.");
												// Send SUCCESS as response with jobId and the workerId to track
												// "jobOpenResponse": {
												// // "status": "SUCCESS",
												// // "jobId": jobId,
												// // "workerId": workerId
												// }
												JsonObject jobOpenResponse = new JsonObject();
												jobOpenResponse.addProperty("status", "SUCCESS");
												jobOpenResponse.addProperty("jobId", jobId);
												jobOpenResponse.addProperty("workerId", workerId);
												JsonObject newJobSuccess = new JsonObject();
												newJobSuccess.add("jobOpenResponse", jobOpenResponse);
												sendJsonToConsumerTopic(newJobSuccess, consumerId, context);
												LOG.info("CONSUMER INFORMED OF THE JOB'S SUCCESS. TAKING NO FURTHER "
														+ "ACTIONS.");
											}
										} catch (Exception e) {
											LOG.error("ACCEPTANCE RUNNABLE FACED EXCEPTION {}", e.getMessage());
										}
									}
								};
								// Delay before starting the acceptance Runnable
								int secondsToWaitForAcceptance = 9;
								// Create new scheduled executor
								ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
								// Schedule the acceptance Runnable
								scheduler.schedule(acceptanceRunnable, secondsToWaitForAcceptance, TimeUnit.SECONDS);
								// Shutdown the scheduled executor
								scheduler.shutdown();
								LOG.info("EXECUTION SCHEDULER WAS SHUTDOWN AFTER SCHEDULING ACCEPTANCE RUNNABLE.");
							}
						} catch (Exception e) {
							LOG.error("RECEIPT RUNNABLE FACED EXCEPTION {}", e.getMessage());
						}
					}
				};
				// Delay before starting the receipt Runnable
				int secondsToWaitForReceipt = 2;
				// Create new scheduled executor
				ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
				// Schedule the receipt Runnable
				scheduler.schedule(receiptRunnable, secondsToWaitForReceipt, TimeUnit.SECONDS);
				// Shutdown the scheduled executor
				scheduler.shutdown();
				LOG.info("EXECUTION SCHEDULER WAS SHUTDOWN AFTER SCHEDULING RECEIPT RUNNABLE.");
			} else {
				if (workerIds.isEmpty()) {
					failureReason = "NO 'APPROPRIATELY SKILLED' WORKERS COULD BE FOUND 'ONLINE' IN '" + radius
							+ "' METERS.";
				} else {
					failureReason = "YOU HAVE NOT BEATEN ME. A HUMAN FORGOT ABOUT TEACHING ME HOW TO DO "
							+ "THAT. TO BE EXACT HE DID NOT IMAGINE THAT THIS WILL HAPPEN.";
				}
				LOG.info("JOB REQUEST FAILED BECAUSE {}", failureReason);
				markJobAsFailed(processSqlConnection, jobId);
				saveJobFailureDetail(processSqlConnection, jobId, failureReason);
				// Send FAILURE as response with reason
				// "jobOpenResponse": {
				// // "status": "FAILURE",
				// // "reason": failureReason,
				// }
				JsonObject jobOpenResponse = new JsonObject();
				jobOpenResponse.addProperty("status", "FAILURE");
				jobOpenResponse.addProperty("reason", failureReason);
				JsonObject newJobFailure = new JsonObject();
				newJobFailure.add("jobOpenResponse", jobOpenResponse);
				sendJsonToConsumerTopic(newJobFailure, consumerId, context);
				LOG.info("CONSUMER INFORMED OF THE JOB'S FAILURE, TAKING NO FURTHER ACTIONS.");
			}
		} catch (Exception e) {
			LOG.error("PROCESS FACED EXCEPTION {}", e.getMessage());
		} finally {
			try {
				if (processSqlConnection != null) {
					processSqlConnection.close();
					LOG.info("CLOSED PROCESS SQL CONNECTION.");
				}
			} catch (SQLException sqle) {
				LOG.error("PROCESS FACED SQL EXCEPTION {}", sqle.getMessage());
			}
		}
		// Get process end time
		String processEndDateTime = dateFormat.format(new Date());
		// End logging
		LOG.info("ENDED PROCESSING MESSAGE ID {} IN {} MILLISECONDS AT {}", messageId,
				((System.nanoTime() - processStartNanoseconds) / 1000000), processEndDateTime);
		// Write no output to the output topic
		return null;
	}

	/**
	 * This main method is required for the local run mode, and can be removed or
	 * commented safely should you require to run the function in cluster mode.
	 *
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// Create new local run configuration object for the function
		FunctionConfig functionConfig = new FunctionConfig();
		// Update function configuration values
		functionConfig.setName("JO");
		functionConfig.setInputs(Collections.singleton("job-opener-input"));
		functionConfig.setClassName(JobOpener.class.getName());
		functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
		// functionConfig.setOutput("job-opener-output"); // No output
		functionConfig.setLogTopic("job-opener-logs");
		functionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE);
		functionConfig.setParallelism(1);
		// Build a local runner
		LocalRunner localRunner = LocalRunner.builder().functionConfig(functionConfig).build();
		localRunner.start(false);
	}

}
