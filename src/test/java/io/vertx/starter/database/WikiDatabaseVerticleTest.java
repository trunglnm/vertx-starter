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
	public void crud_operations(TestContext context) {
		Async async = context.async();

		service.createPage("Test", "Some content", context.asyncAssertSuccess(v1 -> {

			service.fetchPage("Test", context.asyncAssertSuccess(json1 -> {
				context.assertTrue(json1.getBoolean("found"));
				context.assertTrue(json1.containsKey("id"));
				context.assertEquals("Some content", json1.getString("rawContent"));

				service.savePage(json1.getInteger("id"), "Yo!", context.asyncAssertSuccess(v2 -> {

					service.fetchAllPages(context.asyncAssertSuccess(array1 -> {
						context.assertEquals(1, array1.size());

						service.fetchPage("Test", context.asyncAssertSuccess(json2 -> {
							context.assertEquals("Yo!", json2.getString("rawContent"));

							service.deletePage(json1.getInteger("id"), v3 -> {

								service.fetchAllPages(context.asyncAssertSuccess(array2 -> {
									context.assertTrue(array2.isEmpty());
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

	@Test
	public void test_fetchAllPagesData(TestContext context) {
		Async async = context.async();

		service.createPage("A", "abc", context.asyncAssertSuccess(p1 -> {
			service.createPage("B", "123", context.asyncAssertSuccess(p2 -> {
				service.fetchAllPagesData(context.asyncAssertSuccess(data -> {

					context.assertEquals(2, data.size());

					JsonObject a = data.get(0);
					context.assertEquals("A", a.getString("NAME"));
					context.assertEquals("abc", a.getString("CONTENT"));

					JsonObject b = data.get(1);
					context.assertEquals("B", b.getString("NAME"));
					context.assertEquals("123", b.getString("CONTENT"));

					async.complete();

				}));
			}));
		}));

		async.awaitSuccess(5000);
	}
}