package job.opener.sdk;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.functions.LocalRunner;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
 * @author AbdulWahhaab Aa'waan
 * @author Hares Mahmood
 * @author Khubaeb Shabbeer
 */
public class JobOpener implements Function<String, String> {

	// Define a 'null' logger
	Logger LOG = null;

	/**
	 * Provides configuration for creating instance(s) of this function for local
	 * run mode, and can be removed or commented safely should you require to run
	 * the function in cluster mode.
	 *
	 * @param args Any arguments for the main function.
	 * @throws Exception Any exception faced.
	 */
	public static void main(String[] args) throws Exception {
		// Create new local run configuration object for the Pulsar Function
		FunctionConfig functionConfig = new FunctionConfig();
		// Update configuration
		functionConfig.setName("JO");
		functionConfig.setInputs(Collections.singleton("job-opener-input"));
		// functionConfig.setOutput("job-opener-output"); // Because the function's
		// process() is returning 'null' output
		functionConfig.setLogTopic("job-opener-logs");
		functionConfig.setClassName(JobOpener.class.getName());
		functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
		functionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE);
		functionConfig.setParallelism(1);
		// Build a local runner
		LocalRunner localRunner = LocalRunner.builder().functionConfig(functionConfig).build();
		localRunner.start(false);
	}

	/**
	 * Gets a new connection to the database.
	 *
	 * @param origin Either of PROCESS / RECEIPT RUNNABLE / ACCEPTANCE RUNNABLE,
	 *               that needs the new connection.
	 * @return A connection to the database.
	 * @throws SQLException Any SQL exception faced.
	 */
	private Connection getNewDatabaseConnection(String origin) throws SQLException {
		// Save database details
		final String DB_URL = "jdbc:mysql://localhost/hyperworldly?serverTimezone=UTC";
		final String USERNAME = "hyperworldly";
		final String PASSWORD = "bPpXfhoT2ZkzQOFP";
		// Open new connection
		Connection sqlConnection = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
		LOG.info("{} CONNECTED TO DATABASE.", origin);
		return sqlConnection;
	}

	/**
	 * Logs the query part from a Prepared Statement.
	 *
	 * @param preparedStatement Any prepared statement.
	 */
	private void logQueryFromPreparedStatement(PreparedStatement preparedStatement) {
		// Get SQL query from the statement
		String preparedStatementAsString = preparedStatement.toString();
		String preparedSqlQuery = preparedStatementAsString.substring(preparedStatementAsString.indexOf(":") + 2);
		LOG.info("QUERY IN THE PREPARED STATEMENT IS: {}", preparedSqlQuery);
	}

	/**
	 * Prepares a comma-separated string, from an array of integers.
	 *
	 * @param integerArray Any array of integers.
	 * @return Integers (separated by commas) as string.
	 */
	private String makeCommaSeparatedStringFromIntArray(int[] integerArray) {
		String commaSeparatedString;
		commaSeparatedString = "";
		if (integerArray.length > 0) {
			StringBuilder sb = new StringBuilder();
			for (int integer : integerArray) {
				sb.append(integer).append(",");
			}
			commaSeparatedString = sb.deleteCharAt(sb.length() - 1).toString();
		}
		LOG.info("COMMA-SEPARATED STRING PREPARED FROM INTEGER ARRAY IS: {}.", commaSeparatedString);
		return commaSeparatedString;
	}

	/**
	 * Saves a new job to the `job` table in database.
	 *
	 * @param sqlConnection SQL connection to use.
	 * @param consumerId    Consumer who requested the job.
	 * @param proficiencyId Proficiency level required in the job.
	 * @return DB ID for the saved job.
	 * @throws Exception Any exception faced.
	 */
	private int saveNewJob(Connection sqlConnection, int consumerId, int proficiencyId) throws Exception {
		// Prepare 'insert job' statement
		String insertJobQuery = "INSERT INTO `job` (`consumer_id`, `proficiency_id`, `status`, `added_on`) VALUES (?, ?,'OPENED', NOW())";
		PreparedStatement jobStatement = sqlConnection.prepareStatement(insertJobQuery,
				Statement.RETURN_GENERATED_KEYS);
		jobStatement.setInt(1, consumerId);
		jobStatement.setInt(2, proficiencyId);
		logQueryFromPreparedStatement(jobStatement);
		// Execute the statement
		jobStatement.execute();
		// Get and return generated id
		int jobId = 0;
		ResultSet resultSet = jobStatement.getGeneratedKeys();
		if (resultSet.next()) {
			jobId = resultSet.getInt(1);
		}
		LOG.info("SAVED JOB UNDER ID {} INTO `jobs` TABLE.", jobId);
		return jobId;
	}

	/**
	 * Saves required tasks for a job with respective quantities in `jobs_task`
	 * table in database.
	 *
	 * @param sqlConnection          SQL connection to use.
	 * @param jobId                  Job against which tasks and quantities are to
	 *                               be saved.
	 * @param requiredTaskIds        Tasks required in a job as array, in the same
	 *                               order as in quantities.
	 * @param requiredTaskQuantities Quantities of each task in a job as array, in
	 *                               the same order as in tasks.
	 */
	private void saveJobTasksAndQuantities(Connection sqlConnection, int jobId, int[] requiredTaskIds,
			int[] requiredTaskQuantities) throws SQLException {
		// Prepare 'insert job task' statement
		String insertJobTaskQuery = "INSERT INTO `jobs_task` (`job_id`, `task_id`, `quantity`, `added_on`) VALUES"
				+ " (?, ?, ?, NOW())";
		PreparedStatement jobTaskStatement = sqlConnection.prepareStatement(insertJobTaskQuery,
				Statement.RETURN_GENERATED_KEYS);
		for (int i = 0; i < requiredTaskIds.length; i++) {
			jobTaskStatement.setInt(1, jobId);
			jobTaskStatement.setInt(2, requiredTaskIds[i]);
			jobTaskStatement.setInt(3, requiredTaskQuantities[i]);
			logQueryFromPreparedStatement(jobTaskStatement);
			// Execute the statement
			jobTaskStatement.execute();
			// Get generated id
			ResultSet resultSet = jobTaskStatement.getGeneratedKeys();
			int generatedId = 0;
			if (resultSet.next()) {
				generatedId = resultSet.getInt(1);
			}
			LOG.info("SAVED REQUIRED TASK UNDER ID {} AGAINST JOB ID {} INTO `jobs_task` TABLE", generatedId, jobId);
		}
	}

	/**
	 * Saves location for a job in `job_location` table in database.
	 *
	 * @param sqlConnection SQL connection to use.
	 * @param jobId         Job against which location is to be saved.
	 * @param latitude      Latitude part of the job location's coordinates.
	 * @param longitude     Longitude part of the job location's coordinates.
	 * @throws SQLException Any SQL exception faced.
	 */
	private void saveJobLocation(Connection sqlConnection, int jobId, double latitude, double longitude)
			throws SQLException {
		// Prepare 'insert job location' statement
		String insertJobLocationQuery = "INSERT INTO `job_location` (`job_id`, `latitude`, `longitude`, `added_on`) "
				+ "VALUES (?, ?, ?, NOW())";
		PreparedStatement jobLocationStatement = sqlConnection.prepareStatement(insertJobLocationQuery,
				Statement.RETURN_GENERATED_KEYS);
		jobLocationStatement.setInt(1, jobId);
		jobLocationStatement.setDouble(2, latitude);
		jobLocationStatement.setDouble(3, longitude);
		logQueryFromPreparedStatement(jobLocationStatement);
		// Execute the statement
		jobLocationStatement.execute();
		// Get generated id
		ResultSet resultSet = jobLocationStatement.getGeneratedKeys();
		int generatedId = 0;
		if (resultSet.next()) {
			generatedId = resultSet.getInt(1);
		}
		LOG.info("SAVED LOCATION UNDER ID {} AGAINST JOB ID {} INTO `job_location` TABLE", generatedId, jobId);
	}

	/**
	 * Finds online in-proximity workers having required proficiency for all the
	 * tasks in a job.
	 *
	 * @param sqlConnection   SQL connection to use.
	 * @param requiredTaskIds Tasks required in a job.
	 * @param proficiencyId   Proficiency level required in a job.
	 * @param latitude        Latitude part of the job location coordinates.
	 * @param longitude       Longitude part of the job location's coordinates.
	 * @param radius          Radius (in meters) to determine proximity.
	 * @return A list of online, in-proximity, and appropriately skilled workers for
	 *         a job.
	 * @throws SQLException Any SQL exception faced.
	 */
	private List<Integer> findAppropriateWorkers(Connection sqlConnection, int[] requiredTaskIds, int proficiencyId,
			double latitude, double longitude, int radius) throws SQLException {
		String concatenatedRequiredTaskIds = makeCommaSeparatedStringFromIntArray(requiredTaskIds);
		// Set volumetric mean radius of Earth in meters
		int volumetricMeanRadiusOfEarth = 6371008;
		// Prepare 'select appropriate workers' statement (using Spherical Law of
		// Cosines to get nearest worker)
		String selectAppropriateWorkersQuery = "SELECT `worker_id`, (" + volumetricMeanRadiusOfEarth
				+ " * ACOS(LEAST(1.0, COS(" + "RADIANS(" + latitude
				+ ")) * COS(RADIANS(`latitude`)) * COS(RADIANS(`longitude`) - RADIANS(" + longitude
				+ ")) + SIN(RADIANS(" + latitude + ")) * SIN(RADIANS(`latitude`))))) AS `distance` FROM "
				+ "`worker_location` WHERE `worker_id` IN (SELECT `id` FROM `worker` WHERE `id` IN (SELECT `worker_id`"
				+ " FROM `workers_task` WHERE `task_id` IN (" + concatenatedRequiredTaskIds + ") AND `proficiency_id`="
				+ proficiencyId + " GROUP BY `worker_id` HAVING COUNT(DISTINCT `task_id`)=" + requiredTaskIds.length
				+ ") AND `status`='ONLINE') ORDER BY `distance` LIMIT 5";
		PreparedStatement appropriateWorkersStatement = sqlConnection.prepareStatement(selectAppropriateWorkersQuery);
		logQueryFromPreparedStatement(appropriateWorkersStatement);
		// Execute the statement
		ResultSet resultSet = appropriateWorkersStatement.executeQuery();
		// Get and return result
		List<Integer> appropriateWorkers = new ArrayList<Integer>();
		while (resultSet.next()) {
			if (resultSet.getInt(2) < radius) {
				appropriateWorkers.add(resultSet.getInt(1));
			}
		}
		return appropriateWorkers;
	}

	/**
	 * Finds online in-proximity workers having required proficiency for all the
	 * tasks in a job, excluding the workers who previously received the offer for
	 * this job.
	 *
	 * @param sqlConnection   SQL connection to use.
	 * @param requiredTaskIds Tasks required in a job.
	 * @param proficiencyId   Proficiency level required in a job.
	 * @param latitude        Latitude part of the job location coordinates.
	 * @param longitude       Longitude part of the job location's coordinates.
	 * @param radius          Radius (in meters) to determine proximity.
	 * @param jobId           Job against which an offer was sent previously.
	 * @return A list of online, in-proximity, and appropriately skilled workers for
	 *         a job.
	 * @throws SQLException Any SQL exception faced.
	 */
	private List<Integer> findAppropriateWorkersAgain(Connection sqlConnection, int[] requiredTaskIds,
			int proficiencyId, double latitude, double longitude, int radius, int jobId) throws SQLException {
		String concatenatedRequiredTaskIds = makeCommaSeparatedStringFromIntArray(requiredTaskIds);
		// Set volumetric mean radius of Earth in meters
		int volumetricMeanRadiusOfEarth = 6371008;
		// Prepare 'select appropriate workers again' statement (using Spherical Law of
		// Cosines to get nearest to worker)
		String selectAppropriateWorkersAgainQuery = "SELECT `worker_id`, (" + volumetricMeanRadiusOfEarth + " * ACOS"
				+ "(LEAST(1.0, COS(RADIANS(" + latitude + ")) * COS(RADIANS(`latitude`)) * COS(RADIANS(`longitude`) - "
				+ "RADIANS(" + longitude + ")) + SIN(RADIANS(" + latitude + ")) * SIN(RADIANS(`latitude`))))) AS "
				+ "`distance` FROM `worker_location` WHERE `worker_id` IN (SELECT `id` FROM `worker` WHERE `id` IN "
				+ "(SELECT `worker_id` FROM `workers_task` WHERE `task_id` IN (" + concatenatedRequiredTaskIds + ") "
				+ "AND `proficiency_id`=" + proficiencyId + " AND `worker_id` NOT IN (SELECT `worker_id` FROM "
				+ "`jobs_receivers` WHERE `job_id`=" + jobId + ") GROUP BY `worker_id` HAVING COUNT(DISTINCT `task_id`"
				+ ")=" + requiredTaskIds.length + ") AND `status`='ONLINE') ORDER BY `distance` LIMIT 5";
		PreparedStatement appropriateWorkersAgainStatement = sqlConnection
				.prepareStatement(selectAppropriateWorkersAgainQuery);
		logQueryFromPreparedStatement(appropriateWorkersAgainStatement);
		// Execute the statement
		ResultSet resultSet = appropriateWorkersAgainStatement.executeQuery();
		// Get and return result
		List<Integer> appropriateWorkers = new ArrayList<Integer>();
		while (resultSet.next()) {
			if (resultSet.getInt(2) < radius) {
				appropriateWorkers.add(resultSet.getInt(1));
			}
		}
		return appropriateWorkers;
	}

	/**
	 * Saves workers offered a job, in `jobs_offerees` table in the database.
	 *
	 * @param sqlConnection SQL connection to use.
	 * @param jobId         Job which has been offered.
	 * @param workerIds     All the workers who were sent a job offer.
	 * @throws SQLException Any SQL exception faced.
	 */
	private void saveJobOfferees(Connection sqlConnection, int jobId, List<Integer> workerIds) throws SQLException {
		// Prepare 'insert job offeree' statement
		String insertJobOfferee = "INSERT INTO `jobs_offerees` (`job_id`, `worker_id`, `added_on`) "
				+ "VALUES (?, ?, NOW())";
		PreparedStatement jobOffereeStatement = sqlConnection.prepareStatement(insertJobOfferee);
		for (int aWorkerId : workerIds) {
			jobOffereeStatement.setInt(1, jobId);
			jobOffereeStatement.setInt(2, aWorkerId);
			logQueryFromPreparedStatement(jobOffereeStatement);
			// Execute the statement
			jobOffereeStatement.execute();
			// Prepare 'update worker status' statement
			String updateWorkerStatus = "UPDATE `worker` SET `status`='ON-OFFER', `last_updated_on`=NOW() WHERE "
					+ "`worker_id`=?";
			PreparedStatement workerStatusStatement = sqlConnection.prepareStatement(updateWorkerStatus);
			workerStatusStatement.setInt(1, aWorkerId);
			logQueryFromPreparedStatement(workerStatusStatement);
			// Execute the statement
			workerStatusStatement.execute();
			// Prepare 'insert worker status log' statement
			String insertWorkerStatusLog = "INSERT INTO `workers_statuses` (`worker_id`, `status`, `added_on`) VALUES"
					+ " (?, ?, NOW())";
			PreparedStatement workerStatusLogStatement = sqlConnection.prepareStatement(insertWorkerStatusLog);
			workerStatusLogStatement.setInt(1, aWorkerId);
			workerStatusLogStatement.setString(2, "ON-OFFER");
			logQueryFromPreparedStatement(workerStatusLogStatement);
			// Execute the statement
			workerStatusLogStatement.execute();
		}
		LOG.info("SAVED JOB AND THE WORKERS IT WAS OFFERED TO, IN `jobs_offerees` TABLE");
	}

	/**
	 * Prepares a job offer or open request, as a JSON object.
	 *
	 * @param jobAsJsonObject Job offer as a JSON object.
	 * @param jobId           Job which has been offered.
	 * @param jobRequest      Whether the offer is being processed for the first
	 *                        time or again.
	 * @return Offer as a JSON object.
	 */
	private JsonObject prepareOfferOrJobRequest(JsonObject jobAsJsonObject, int jobId, boolean jobRequest) {
		// Remove (any/old) job id if present
		if (jobAsJsonObject.has("jobId")) {
			jobAsJsonObject.remove("jobId");
		}
		// Add the latest job id
		jobAsJsonObject.addProperty("jobId", jobId);
		// Create new container JSON object
		JsonObject jobOrOfferAsJson = new JsonObject();
		if (jobRequest) {
			jobOrOfferAsJson.add("jobOpenRequest", jobAsJsonObject);
		} else {
			jobOrOfferAsJson.add("instantJobOffer", jobAsJsonObject);
		}
		// Get job JSON as string
		LOG.info("JOB OR OFFER TO SEND IS {}", jobOrOfferAsJson.toString());
		return jobOrOfferAsJson;
	}

	/**
	 * Checks - in the `jobs_receivers` table in database - if a job offer sent to
	 * workers was actually received by any of them.
	 *
	 * @param sqlConnection SQL connection to use.
	 * @param jobId         Job against which receipt is to be checked.
	 * @return Whether or not any worker received the job.
	 * @throws SQLException Any SQL exceptions faced.
	 */
	private boolean wasJobReceivedByAnyWorker(Connection sqlConnection, int jobId) throws SQLException {
		boolean received = false;
		// Prepare SQL statement
		String selectWorkerQuery = "SELECT `worker_id` FROM `jobs_receivers` WHERE `job_id`=?";
		PreparedStatement workerStatement = sqlConnection.prepareStatement(selectWorkerQuery);
		workerStatement.setInt(1, jobId);
		logQueryFromPreparedStatement(workerStatement);
		// Execute the prepared statement and get result
		ResultSet resultSet = workerStatement.executeQuery();
		if (resultSet.next()) {
			received = true;
		}
		// Those who did not receive the job offer and have no status changes in
		// previous 2 second, are OFFLINE
		// TODO: The following query should update the worker status in the worker table
		// and then write to the log too.
		String updateWorkerStatusQuery;
		if (received) {
			updateWorkerStatusQuery = "UPDATE `workers_statuses` SET `status`='OFFLINE', `last_updated_on`=NOW() WHERE"
					+ " `worker_id` IN (SELECT `worker_id` FROM `jobs_offerees` WHERE `worker_id` NOT IN (SELECT"
					+ " `worker_id` FROM `jobs_receivers` WHERE `job_id`=" + jobId + ")) AND `status`='ON-OFFER' AND "
					+ "`last_updated_on` > (SELECT DATE_SUB(NOW(), INTERVAL 2 SECOND))";
		} else {
			updateWorkerStatusQuery = "UPDATE `workers_statuses` SET `status`='OFFLINE', `last_updated_on`=NOW() WHERE"
					+ " `worker_id` IN (SELECT `worker_id` FROM `jobs_offerees` WHERE `job_id`=" + jobId + ") AND "
					+ "`status`='ON-OFFER' AND `last_updated_on` > (SELECT DATE_SUB(NOW(), INTERVAL 2 SECOND))";
		}
		PreparedStatement workerStatusStatement = sqlConnection.prepareStatement(updateWorkerStatusQuery);
		logQueryFromPreparedStatement(workerStatusStatement);
		// Execute the statement
		workerStatusStatement.execute();
		return received;
	}

	/**
	 * Checks - in the `job` table in the database - if a job offer sent to a worker
	 * was actually accepted.
	 *
	 * @param sqlConnection SQL connection to use.
	 * @param jobId         Job against which acceptance is to be checked.
	 * @return Whether or not any worker accepted a job offer.
	 * @throws SQLException Any SQL exception faced.
	 */
	private boolean wasJobAcceptedByAnyWorker(Connection sqlConnection, int jobId) throws SQLException {
		boolean accepted = false;
		// Prepare SQL statement
		String selectWorkerQuery = "SELECT `worker_id` FROM `job` WHERE `id`=?";
		PreparedStatement workerStatement = sqlConnection.prepareStatement(selectWorkerQuery);
		workerStatement.setInt(1, jobId);
		logQueryFromPreparedStatement(workerStatement);
		// Execute the prepared statement and get result
		ResultSet resultSet = workerStatement.executeQuery();
		if (resultSet.next()) {
			accepted = true;
		}
		// Those who received the offer, but did not accept or reject it, and have no
		// status changes in previous 10 seconds, should be ONLINE to get a new offer
		// TODO: The following query should update the worker status in the worker table
		// and then write to the log too.
		String updateWorkerStatusQuery;
		if (accepted) {
			updateWorkerStatusQuery = "UPDATE `workers_statuses` SET `status`='ONLINE', `last_updated_on`=NOW() WHERE "
					+ "`worker_id` IN (SELECT `worker_id` FROM `jobs_receivers` WHERE `worker_id` NOT IN (SELECT "
					+ "`worker_id` FROM `job` WHERE `id`=" + jobId + ")) AND `status`='ON-OFFER' AND "
					+ "`last_updated_on` > (SELECT DATE_SUB(NOW(), INTERVAL 12 SECOND))";
		} else {
			updateWorkerStatusQuery = "UPDATE `workers_statuses` SET `status`='ONLINE', `last_updated_on`=NOW() WHERE "
					+ "`worker_id` IN (SELECT `worker_id` FROM `jobs_receivers` WHERE `job_id`=" + jobId + ") AND "
					+ "`status`='ON-OFFER' AND `last_updated_on` > (SELECT DATE_SUB(NOW(), INTERVAL 12 SECOND))";
		}
		PreparedStatement workerStatusStatement = sqlConnection.prepareStatement(updateWorkerStatusQuery);
		logQueryFromPreparedStatement(workerStatusStatement);
		// Execute the prepared statement and get result
		workerStatusStatement.execute();
		return accepted;
	}

	/**
	 * Marks job status as FAILED in the `job` table in database.
	 *
	 * @param sqlConnection SQL connection to use.
	 * @param jobId         Job that failed.
	 * @throws SQLException Any SQL exception faced.
	 */
	private void markJobAsFailed(Connection sqlConnection, int jobId) throws SQLException {
		// Prepare SQL statement
		String updateJobQuery = "UPDATE `job` SET `status`=?, `last_updated_on`=NOW() WHERE `id`=?";
		PreparedStatement jobStatement = sqlConnection.prepareStatement(updateJobQuery);
		jobStatement.setString(1, "FAILED");
		jobStatement.setInt(2, jobId);
		logQueryFromPreparedStatement(jobStatement);
		// Execute the prepared statement
		jobStatement.executeUpdate();
		LOG.info("UPDATED STATUS FOR JOB ID {} AS FAILED IN `job` TABLE", jobId);
	}

	/**
	 * Saves the failure detail of a job in the `job_failure_detail` table in the
	 * database
	 *
	 * @param sqlConnection SQL connection to use.
	 * @param jobId         Job that failed.
	 * @throws SQLException Any SQL exception faced.
	 */
	private void saveJobFailureDetail(Connection sqlConnection, int jobId, String failureReason) throws SQLException {
		// Prepare SQL statement
		String sqlStringToPrepare = "INSERT INTO `job_failure_detail` (`job_id`, `details`, `added_on`) "
				+ "VALUES (?, ?, NOW())";
		PreparedStatement preparedStatement = sqlConnection.prepareStatement(sqlStringToPrepare,
				Statement.RETURN_GENERATED_KEYS);
		preparedStatement.setInt(1, jobId);
		preparedStatement.setString(2, failureReason);
		logQueryFromPreparedStatement(preparedStatement);
		// Execute the prepared statement
		preparedStatement.execute();
		// Get generated id
		ResultSet resultSet = preparedStatement.getGeneratedKeys();
		int generatedId = 0;
		if (resultSet.next()) {
			generatedId = resultSet.getInt(1);
		}
		LOG.info("SAVED JOB FAILURE DETAIL UNDER ID {} INTO `job` TABLE", generatedId);
	}

	/**
	 * Gets the total elapsed time (in seconds) since a provided date and time.
	 *
	 * @param timeFromPast Any date and time (from past) as string.
	 * @return The difference between now and provided time (from past).
	 * @throws ParseException Any exception faced while parsing.
	 */
	private int getTotalElapsedTimeInSeconds(String timeFromPast) throws ParseException {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date currentDate = new Date();
		Date requestedOnDate = dateFormat.parse(timeFromPast);
		long difference = currentDate.getTime() - requestedOnDate.getTime();
		return (int) (difference / (1000));
	}

	/**
	 * Gets the worker who accepted a job.
	 *
	 * @param sqlConnection SQL connection to use.
	 * @param jobId         Job which has been accepted.
	 * @return Worker who accepted the job.
	 * @throws SQLException Any SQL exception faced.
	 */
	private int getWorkerForJob(Connection sqlConnection, int jobId) throws SQLException {
		// Prepare SQL statement
		String selectWorkerQuery = "SELECT `worker_id` FROM `job` WHERE `id`=?";
		PreparedStatement workerStatement = sqlConnection.prepareStatement(selectWorkerQuery);
		workerStatement.setInt(1, jobId);
		logQueryFromPreparedStatement(workerStatement);
		// Execute the prepared statement and get result
		ResultSet resultSet = workerStatement.executeQuery();
		int workerId = 0;
		if (resultSet.next()) {
			workerId = resultSet.getInt(1);
		}
		return workerId;
	}

	/**
	 * Sends a message to a consumer.
	 *
	 * @param jsonMessage Message to send as JSON.
	 * @param consumerId  Any consumer.
	 * @param context     The 'Context' provided by Pulsar Functions SDK.
	 * @throws PulsarClientException Any exception faced by Pulsar client.
	 */
	private void sendMessageToConsumer(JsonObject jsonMessage, int consumerId, Context context)
			throws PulsarClientException {
		String consumerTopic = "consumer-" + consumerId;
		context.newOutputMessage(consumerTopic, Schema.STRING).value(jsonMessage.toString()).send();
		LOG.info("SENT JSON MESSAGE TO CONSUMER TOPIC {}", consumerTopic);
	}

	/**
	 * Sends a message to a worker.
	 *
	 * @param jsonMessage Any JSON object.
	 * @param workerId    Any worker's DB ID.
	 * @param context     The 'Context' provided by 'Pulsar Functions SDK'.
	 * @throws PulsarClientException Any exception faced by Pulsar (producer)
	 *                               client.
	 */
	private void sendMessageToWorker(JsonObject jsonMessage, int workerId, Context context)
			throws PulsarClientException {
		// Define and initialize worker's topic
		String workerTopic = "worker-" + workerId;
		// Sends received message
		context.newOutputMessage(workerTopic, Schema.STRING).value(jsonMessage.toString()).send();
		LOG.info("SENT MESSAGE TO WORKER TOPIC {}", workerTopic);
	}

	/**
	 * Processes every message received on this Pulsar Function's input topic.
	 *
	 * @param input   The message received.
	 * @param context The 'Context' provided by 'Pulsar Functions SDK'.
	 * @return The message to be written to the output topic, or null.
	 */
	@Override
	public String process(String input, Context context) {
		// Save process start nano time
		long processStartNanoseconds = System.nanoTime();
		// Save a date format
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		// Save (formatted) process start date and time
		String processStartDateTime = dateFormat.format(new Date());
		// Load the logger
		LOG = context.getLogger();
		// Save the received message
		String message = context.getCurrentRecord().getMessage().toString();
		// Save message ID (Is that the standard way to get it?)
		String messageId = message.substring(message.lastIndexOf("@") + 1, message.length() - 1);
		// Start logging
		LOG.info("STARTED PROCESSING MESSAGE ID {} ON {} WITH INPUT {}.", messageId, processStartDateTime, input);
		// Save a null connection
		Connection processSqlConnection = null;
		try {
			// Define job variables
			int jobId, consumerId, proficiencyId;
			double latitude, longitude;
			String requestedOn;
			String failureReason = null;
			LOG.info("STARTED PARSING NEW JOB REQUEST");
			// Save the input as JSON
			JsonObject inputAsJson = new JsonParser().parse(input).getAsJsonObject();
			// Extract and save job request as a JSON object
			final JsonObject jobAsJsonObject = inputAsJson.getAsJsonObject("jobOpenRequest");
			// Extract values from job object
			consumerId = jobAsJsonObject.get("consumerId").getAsInt();
			proficiencyId = jobAsJsonObject.get("proficiencyId").getAsInt();
			requestedOn = jobAsJsonObject.get("requestedOn").getAsString();
			// Extract job location as JSON array
			JsonArray location = jobAsJsonObject.getAsJsonArray("location");
			latitude = location.get(0).getAsDouble();
			longitude = location.get(1).getAsDouble();
			// Extract required tasks and quantities as JSON array
			JsonArray requiredTaskIdsAndQuantities = jobAsJsonObject.getAsJsonArray("requiredTaskIdsAndQuantities");
			int requiredTasksSize = requiredTaskIdsAndQuantities.size();
			// Save tasks to an array
			int[] requiredTaskIds = new int[requiredTasksSize];
			for (int i = 0; i < requiredTasksSize; i++) {
				JsonArray requiredTaskIdAndQuantity = (JsonArray) requiredTaskIdsAndQuantities.get(i);
				for (int j = 0; j < requiredTasksSize; j++) {
					requiredTaskIds[i] = requiredTaskIdAndQuantity.get(0).getAsInt();
				}
			}
			// Save quantities to another array
			int[] requiredTaskQuantities = new int[requiredTasksSize];
			for (int i = 0; i < requiredTasksSize; i++) {
				JsonArray requiredTaskIdAndQuantity = (JsonArray) requiredTaskIdsAndQuantities.get(i);
				for (int j = 0; j < requiredTasksSize; j++) {
					requiredTaskQuantities[i] = requiredTaskIdAndQuantity.get(1).getAsInt();
				}
			}
			// Save the search radius in meters
			int radius = 10000;
			// Get DB connection
			processSqlConnection = getNewDatabaseConnection("PROCESS");
			// Create an empty list for workers who'll be sent the offer
			List<Integer> workerIds = new ArrayList<Integer>();
			// Check if the message being processed is a new job offer
			if (!input.contains("jobId")) {
				LOG.info("THIS IS THE FIRST ATTEMPT ON PROCESSING MESSAGE ID {}.", messageId);
				LOG.info(
						"consumerId IS {}, proficiencyId IS {}, requestedOn IS {}, latitude IS {}, longitude IS {}, "
								+ "requiredTaskIds ARE {}, requiredTaskQuantities ARE {}",
						consumerId, proficiencyId, requestedOn, latitude, longitude, requiredTaskIds,
						requiredTaskQuantities);
				// Save the new job in database
				jobId = saveNewJob(processSqlConnection, consumerId, proficiencyId);
				// Save the required tasks with respective quantities against job
				saveJobTasksAndQuantities(processSqlConnection, jobId, requiredTaskIds, requiredTaskQuantities);
				// Save the location for the job
				saveJobLocation(processSqlConnection, jobId, latitude, longitude);
				// Get appropriate workers to offer the job to
				workerIds = findAppropriateWorkers(processSqlConnection, requiredTaskIds, proficiencyId, latitude,
						longitude, radius);
			} else {
				LOG.info("THIS IS A REPEAT ATTEMPT ON PROCESSING MESSAGE ID {}", messageId);
				// Parse the sent offer message
				jobId = jobAsJsonObject.get("jobId").getAsInt();
				int totalElapsedTime = getTotalElapsedTimeInSeconds(requestedOn);
				if (totalElapsedTime <= 36) {
					LOG.info(
							"jobId IS {}, consumerId IS {}, proficiencyId IS {}, "
									+ "requestedOn IS {}, latitude IS {}, longitude IS {}, requiredTaskIds ARE {}",
							jobId, consumerId, proficiencyId, requestedOn, latitude, longitude, requiredTaskIds);
					// Get IDs of new matching workers online nearby
					workerIds = findAppropriateWorkersAgain(processSqlConnection, requiredTaskIds, proficiencyId,
							latitude, longitude, radius, jobId);
				} else {
					failureReason = "36 SECONDS HAVE ELAPSED, AND NO WORKER AVAILABLE NEARBY ACCEPTED THE JOB.";
				}
			}
			// Ensure that we have some worker IDs
			if (!workerIds.isEmpty()) {
				// Save the worker IDs against job's id for future reference
				saveJobOfferees(processSqlConnection, jobId, workerIds);
				// Prepare a job offer message containing the workerId and the jobId
				JsonObject jobOffer = prepareOfferOrJobRequest(jobAsJsonObject, jobId, false);
				// Send the job offer to each of the matching workers' topic
				for (int aWorkerId : workerIds) {
					sendMessageToWorker(jobOffer, aWorkerId, context);
				}
				// Build a future Runnable that checks for job receipt
				Runnable receiptRunnable = new Runnable() {
					@Override
					public void run() {
						try {
							// Get database connection
							Connection receiptRunnableSqlConnection = getNewDatabaseConnection("RECEIPT RUNNABLE");
							// Check if a sent job was marked as received
							boolean jobReceived = wasJobReceivedByAnyWorker(receiptRunnableSqlConnection, jobId);
							// Close database connection
							receiptRunnableSqlConnection.close();
							LOG.info("CLOSED RECEIPT RUNNABLE DATABASE CONNECTION.");
							// Prepare job open request
							JsonObject jobOpenRequest = prepareOfferOrJobRequest(jobAsJsonObject, jobId, true);
							if (!jobReceived) {
								LOG.info("THE JOB HAS NOT BEEN MARKED RECEIVED BY ANY OF THE WORKERS IT WAS SENT TO. "
										+ "REPEATING PROCESS.");
								// Repeat the whole process with
								process(jobOpenRequest.toString(), context);
							} else {
								LOG.info("THE JOB HAS BEEN MARKED RECEIVED BY ONE OF THE WORKERS IT WAS SENT TO.");
								// Build a future Runnable that checks for job acceptance
								Runnable acceptanceRunnable = new Runnable() {
									@Override
									public void run() {
										try {
											// Get database connection
											Connection acceptanceRunnableSqlConnection = getNewDatabaseConnection(
													"ACCEPTANCE RUNNABLE");
											// Check if a sent offer was marked as accepted
											boolean jobAccepted = wasJobAcceptedByAnyWorker(
													acceptanceRunnableSqlConnection, jobId);
											int workerId = getWorkerForJob(acceptanceRunnableSqlConnection, jobId);
											// Close database connection
											acceptanceRunnableSqlConnection.close();
											LOG.info("CLOSED ACCEPTANCE RUNNABLE DATABASE CONNECTION");
											if (!jobAccepted) {
												LOG.info("THE JOB HAS NOT BEEN MARKED ACCEPTED BY ANY OF THE WORKERS IT"
														+ " WAS SENT TO. REPEATING PROCESS.");
												process(jobOpenRequest.toString(), context);
											} else {
												LOG.info("THE JOB HAS BEEN MARKED ACCEPTED BY THE WORKER IT WAS "
														+ "SENT TO.");
												// Send SUCCESS as response with jobId and the workerId to track
												// "jobOpenResponse": {
												// // "status": "SUCCESS",
												// // "jobId": jobId,
												// // "workerId": workerId
												// }
												JsonObject jobOpenResponseBody = new JsonObject();
												jobOpenResponseBody.addProperty("status", "SUCCESS");
												jobOpenResponseBody.addProperty("jobId", jobId);
												jobOpenResponseBody.addProperty("workerId", workerId);
												JsonObject jobOpenResponse = new JsonObject();
												jobOpenResponse.add("jobOpenResponse", jobOpenResponseBody);
												sendMessageToConsumer(jobOpenResponse, consumerId, context);
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
				LOG.info("JOB REQUEST FAILED BECAUSE {}", failureReason);
				markJobAsFailed(processSqlConnection, jobId);
				saveJobFailureDetail(processSqlConnection, jobId, failureReason);
				// Send FAILURE as response with reason
				// "jobOpenResponse": {
				// // "status": "FAILURE",
				// // "reason": failureReason,
				// }
				JsonObject jobOpenResponseBody = new JsonObject();
				jobOpenResponseBody.addProperty("status", "FAILURE");
				jobOpenResponseBody.addProperty("reason", failureReason);
				JsonObject jobOpenResponse = new JsonObject();
				jobOpenResponse.add("jobOpenResponse", jobOpenResponseBody);
				sendMessageToConsumer(jobOpenResponse, consumerId, context);
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

}
