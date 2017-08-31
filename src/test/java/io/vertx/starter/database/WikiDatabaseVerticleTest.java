package io.vertx.starter.database;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Created by trunglnm on 8/31/17.
 */

@RunWith(VertxUnitRunner.class)
public class WikiDatabaseVerticleTest {

	private Vertx vertx;
	private WikiDatabaseService service;

	@Before
	public void prepare(TestContext context) throws InterruptedException {

		vertx = Vertx.vertx();

		JsonObject conf = new JsonObject()
				.put(WikiDataBaseVerticle.CONFIG_WIKIDB_JDBC_URL, "jdbc:hsqldb:mem:testdb;shutdown=true")
				.put(WikiDataBaseVerticle.CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE, 4);

		vertx.deployVerticle(new WikiDataBaseVerticle(), new DeploymentOptions().setConfig(conf),
			context.asyncAssertSuccess(id ->
				service = WikiDatabaseService.createProxy(vertx, WikiDataBaseVerticle.CONFIG_WIKIDB_QUEUE)));

	}

	@After
	public void finish(TestContext context) {
		vertx.close(context.asyncAssertSuccess());
	}

	@Test
	public void async_behavior(TestContext context) {
		Vertx vertx = Vertx.vertx();
		context.assertEquals("foo", "foo");
		Async a1 = context.async();
		Async a2 = context.async(3);
		vertx.setTimer(100, n -> a1.complete());
		vertx.setTimer(100, n -> a2.countDown());
	}

	@Test
	public void crud_operation(TestContext context) {
		Async async = context.async();

		service.createPage("Test", "Some content", context.asyncAssertSuccess (v1 -> {
			service.fetchPage("Test", context.asyncAssertSuccess(json -> {
				context.assertTrue(json.getBoolean("found"));
				context.assertTrue(json.containsKey("id"));
				context.assertEquals("Some content", json.getString("rawContent"));

				service.savePage(json.getInteger("id"), "Yeah!", context.asyncAssertSuccess(v2 -> {

					service.fetchAllPages(context.asyncAssertSuccess(arr1 -> {
						context.assertEquals(1, arr1.size());

						service.fetchPage("Test", context.asyncAssertSuccess(json2 -> {
							context.assertEquals("Yeah!", json2.getString("rawContent"));

							service.deletePage(json.getInteger("id"), v3 -> {

								service.fetchAllPages(context.asyncAssertSuccess(arr2 -> {
									context.assertTrue(arr2.isEmpty());
									async.complete();
								}));
							});
						}));
					}));
				}));
			}));
		}));

		async.awaitSuccess(5000);
	}

}
