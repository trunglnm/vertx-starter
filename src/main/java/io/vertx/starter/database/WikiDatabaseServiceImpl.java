package io.vertx.starter.database;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.stream.Collectors;

/**
 * Created by trunglnm on 8/30/17.
 */
public class WikiDatabaseServiceImpl implements WikiDatabaseService {

	private static final Logger LOGGER = LoggerFactory.getLogger(WikiDatabaseServiceImpl.class);

	private final HashMap<SqlQuery, String> sqlQueries;
	private final JDBCClient dbClient;

	WikiDatabaseServiceImpl(JDBCClient dbClient, HashMap<SqlQuery, String> sqlQueries,
							Handler<AsyncResult<WikiDatabaseService>> readyHandler) {
		this.dbClient = dbClient;
		this.sqlQueries = sqlQueries;

		dbClient.getConnection(asyncResult -> {
			if (asyncResult.failed()) {
				LOGGER.error("Could not open a database connection", asyncResult.cause());
				readyHandler.handle(Future.failedFuture(asyncResult.cause()));
			} else {
				SQLConnection connection = asyncResult.result();
				connection.execute(sqlQueries.get(SqlQuery.CREATE_PAGES_TABLE), resultHandler -> {
					connection.close();

					if (resultHandler.failed()) {
						LOGGER.error("Database preparation error", resultHandler.cause());
						readyHandler.handle(Future.failedFuture(resultHandler.cause()));
					} else {
						readyHandler.handle(Future.succeededFuture(this));
					}
				});
			}

		});
	}

	@Override
	public WikiDatabaseService fetchAllPages(Handler<AsyncResult<JsonArray>> resultHandler) {

		dbClient.getConnection(asyncResult -> {
			if (asyncResult.succeeded()) {
				SQLConnection connection = asyncResult.result();
				connection.query(sqlQueries.get(SqlQuery.ALL_PAGES), result -> {
					connection.close();

					if (result.succeeded()) {
						JsonArray pages = new JsonArray(result.result().getResults()
								.stream()
								.map(json -> json.getString(0))
								.sorted()
								.collect(Collectors.toList()));

						resultHandler.handle(Future.succeededFuture(pages));
					} else {
						LOGGER.error("Database query error", result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
			} else {
				LOGGER.error("Database query error", asyncResult.cause());
				resultHandler.handle(Future.failedFuture(asyncResult.cause()));
			}
		});

		return this;
	}

	@Override
	public WikiDatabaseService fetchPage(String name, Handler<AsyncResult<JsonObject>> resultHandler) {

		dbClient.getConnection(asyncResult -> {
			if (asyncResult.succeeded()) {

				SQLConnection connection = asyncResult.result();
				connection.queryWithParams(sqlQueries.get(SqlQuery.GET_PAGE), new JsonArray().add(name), result -> {
					connection.close();

					if (result.succeeded()) {
						JsonObject response = new JsonObject();
						ResultSet resultSet = result.result();
						if (resultSet.getNumRows() == 0) {
							response.put("found", false);
						} else {
							response.put("found", true);
							JsonArray row = resultSet.getResults().get(0);
							response.put("id", row.getInteger(0));
							response.put("rawContent", row.getString(1));
						}

						resultHandler.handle(Future.succeededFuture(response));
					} else {
						LOGGER.error("Database query error", result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
			} else {
				LOGGER.error("Database query error", asyncResult.cause());
				resultHandler.handle(Future.failedFuture(asyncResult.cause()));
			}
		});

		return this;
	}

	@Override
	public WikiDatabaseService createPage(String title, String markdown, Handler<AsyncResult<Void>> resultHandler) {

		dbClient.getConnection(asyncResult -> {
			if (asyncResult.succeeded()) {

				JsonArray data = new JsonArray().add(title).add(markdown);
				SQLConnection connection = asyncResult.result();
				connection.updateWithParams(sqlQueries.get(SqlQuery.CREATE_PAGE), data, result -> {
					connection.close();

					if (result.succeeded()) {
						resultHandler.handle(Future.succeededFuture());
					} else {
						LOGGER.error("Database query error", result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
			} else {
				LOGGER.error("Database query error", asyncResult.cause());
				resultHandler.handle(Future.failedFuture(asyncResult.cause()));
			}
		});

		return this;
	}

	@Override
	public WikiDatabaseService savePage(int id, String markdown, Handler<AsyncResult<Void>> resultHandler) {

		dbClient.getConnection(asyncResult -> {
			if (asyncResult.succeeded()) {
				SQLConnection connection = asyncResult.result();
				JsonArray data = new JsonArray().add(id).add(markdown);
				connection.updateWithParams(sqlQueries.get(SqlQuery.SAVE_PAGE), data, result -> {
					connection.close();
					if (result.succeeded()) {
						resultHandler.handle(Future.succeededFuture());
					} else {
						LOGGER.error("Database query error", result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
			} else {
				LOGGER.error("Database query error", asyncResult.cause());
				resultHandler.handle(Future.failedFuture(asyncResult.cause()));
			}
		});

		return this;
	}

	@Override
	public WikiDatabaseService deletePage(int id, Handler<AsyncResult<Void>> resultHandler) {

		dbClient.getConnection(asyncResult -> {
			if (asyncResult.succeeded()) {
				SQLConnection connection = asyncResult.result();
				JsonArray data = new JsonArray().add(id);
				connection.updateWithParams(sqlQueries.get(SqlQuery.DELETE_PAGE), data, result -> {
					connection.close();
					if (result.succeeded()) {
						resultHandler.handle(Future.succeededFuture());
					} else {
						LOGGER.error("Database query error", result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
			} else {
				LOGGER.error("Database query error", asyncResult.cause());
				resultHandler.handle(Future.failedFuture(asyncResult.cause()));
			}
		});

		return this;
	}
}
