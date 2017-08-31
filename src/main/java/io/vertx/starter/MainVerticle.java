package io.vertx.starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.starter.database.WikiDataBaseVerticle;

public class MainVerticle extends AbstractVerticle {

	@Override
	public void start(Future<Void> startFuture) throws Exception {

		Future<String> dbVerticleDeployment = Future.future();
		vertx.deployVerticle(new WikiDataBaseVerticle(), dbVerticleDeployment.completer());

		dbVerticleDeployment.compose(id -> {
			Future<String> httpVerticleDeployment = Future.future();
			vertx.deployVerticle(
					"io.vertx.starter.http.HttpServerVerticle",
					new DeploymentOptions().setInstances(2),
					httpVerticleDeployment.completer());

			return httpVerticleDeployment;

		}).setHandler(asyncResult -> {
			if (asyncResult.succeeded()) {
				startFuture.complete();
			} else {
				startFuture.fail(asyncResult.cause());
			}
		});
	}
}
