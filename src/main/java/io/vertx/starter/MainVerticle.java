package io.vertx.starter;

import com.github.rjeschke.txtmark.Processor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine;
import io.vertx.starter.database.WikiDataBaseVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class MainVerticle extends AbstractVerticle {

  private static final String SQL_CREATE_PAGES_TABLE = "create table if not exists Pages" +
    " (Id integer identity primary key, Name varchar(255) unique, Content clob)";
  private static final String SQL_GET_PAGE = "select Id, Content from Pages where Name = ?";
  private static final String SQL_CREATE_PAGE = "insert into Pages values (NULL, ?, ?)";
  private static final String SQL_SAVE_PAGE = "update Pages set Content = ? where Id = ?";
  private static final String SQL_ALL_PAGES = "select Name from Pages";
  private static final String SQL_DELETE_PAGE = "delete from Pages where Id = ?";
  private static final Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class);
  private static final String EMPTY_PAGE_MARKDOWN =
    "# A new page \n" +
      "\n" +
      "Feel-free to write in Markdown!\n";
  private final FreeMarkerTemplateEngine templateEngine = FreeMarkerTemplateEngine.create();
  private JDBCClient dbClient;

  @Override
  public void start() {
    vertx.createHttpServer()
      .requestHandler(req -> req.response().end("Hello Vert.x!"))
      .listen(8080);
  }

  @Override
  public void start(Future<Void> startFuture) throws Exception {
//    startFuture.complete();

//    Future<Void> steps = prepareDatabase().compose(aVoid -> startHttpServer());
////    steps.setHandler(startFuture.completer());
//    steps.setHandler(asyncResult -> {
//      if (asyncResult.succeeded()) {
//        startFuture.complete();
//      } else {
//        startFuture.fail(asyncResult.cause());
//      }
//    });

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
      if(asyncResult.succeeded()) {
        startFuture.complete();
      } else {
        startFuture.fail(asyncResult.cause());
      }
    });
  }

  private Future<Void> prepareDatabase() {
    Future<Void> future = Future.future();

    dbClient = JDBCClient.createShared(vertx, new JsonObject()
      .put("url", "jdbc:hsqldb:file:db/wiki")
      .put("driver_class", "org.hsqldb.jdbcDriver")
      .put("max_pool_size", 30));

    dbClient.getConnection(asyncResult -> {
      if (asyncResult.failed()) {
        LOGGER.error("Could not open a database connection", asyncResult.cause());
        future.fail(asyncResult.cause());
      } else {
        SQLConnection connection = asyncResult.result();
        connection.execute(SQL_CREATE_PAGES_TABLE, createStatement -> {
          connection.close();
          if (createStatement.failed()) {
            LOGGER.error("Database preparation error", createStatement.cause());
          } else {
            future.complete();
          }
        });
      }
    });

    return future;
  }

  private Future<Void> startHttpServer() {
    Future<Void> future = Future.future();

    HttpServer server = vertx.createHttpServer();

    Router router = Router.router(vertx);
    router.get("/").handler(this::indexHandler);
    router.get("/wiki:/page").handler(this::pageRenderingHandler);
    router.post().handler(BodyHandler.create());
    router.post("/save").handler(this::pageUpdateHandler);
    router.post("/create").handler(this::pageCreateHandler);
    router.post("/delete").handler(this::pageDeletionHandler);

    server
      .requestHandler(router::accept)
      .listen(8080, asyncResult -> {
        if (asyncResult.succeeded()) {
          LOGGER.info("HTTP server running on port 8080");
          future.complete();
        } else {
          LOGGER.error("Could not start a HTTP server", asyncResult.cause());
          future.fail(asyncResult.cause());
        }
      });

    return future;
  }

  private void indexHandler(RoutingContext context) {
    dbClient.getConnection(asyncResult -> {
      if (asyncResult.succeeded()) {
        SQLConnection connection = asyncResult.result();
        connection.query(SQL_ALL_PAGES, resultHandler -> {
          connection.close();
          if (resultHandler.succeeded()) {
            List<String> pages = resultHandler.result()
              .getResults()
              .stream()
              .map(json -> json.getString(0))
              .sorted()
              .collect(Collectors.toList());

            context.put("title", "Wiki home");
            context.put("pages", pages);
            templateEngine.render(context, "templates", "/index.ftl", ar -> {
              if (ar.succeeded()) {
                context.response().putHeader("Context-Type", "text/html");
                context.response().end(ar.result());
              } else {
                context.fail(ar.cause());
              }
            });
          } else {
            context.fail(resultHandler.cause());
          }
        });
      } else {
        context.fail(asyncResult.cause());
      }
    });
  }

  private void pageRenderingHandler(RoutingContext context) {
    String page = context.request().getParam("page");

    dbClient.getConnection(asyncResult -> {
      if(asyncResult.succeeded()) {

        SQLConnection connection = asyncResult.result();
        connection.queryWithParams(SQL_GET_PAGE, new JsonArray().add(page), fetch -> {
          connection.close();

          if(fetch.succeeded()) {

            JsonArray row = fetch.result().getResults()
              .stream()
              .findFirst()
              .orElseGet(() -> new JsonArray().add(-1).add(EMPTY_PAGE_MARKDOWN));
            Integer id = row.getInteger(0);
            String rawContext = row.getString(1);

            context.put("title", page);
            context.put("id", id);
            context.put("newPage", fetch.result().getResults().size() == 0 ? "yes" : "no");
            context.put("rawContent", rawContext);
            context.put("content", Processor.process(rawContext));
            context.put("timestamp", new Date().toString());

            templateEngine.render(context, "templates", "/page.ftl", ar -> {
              if(ar.succeeded()) {
                context.response().putHeader("Context-Type", "text/html");
                context.response().end(ar.result());
              } else {
                context.fail(ar.cause());
              }
            });
          } else  {
            context.fail(fetch.cause());
          }
        });
      } else {
        context.fail(asyncResult.cause());
      }
    });
  }

  private void pageCreateHandler(RoutingContext context) {
    String pageName = context.request().getParam("name");
    String location = "/wiki/" + pageName;
    if(pageName == null || pageName.isEmpty()) {
      location = "/";
    }

    context.response().setStatusCode(303);
    context.response().putHeader("Location", location);
    context.response().end();
  }

  private void pageUpdateHandler(RoutingContext context) {
    String id = context.request().getParam("id");
    String title = context.request().getParam("title");
    String markdown = context.request().getParam("markdown");

    boolean newPage = "yes".equals(context.request().getParam("newPage"));

    dbClient.getConnection(asyncResult -> {
      if(asyncResult.succeeded()) {

        SQLConnection connection = asyncResult.result();
        String sql = newPage ? SQL_CREATE_PAGE : SQL_SAVE_PAGE;
        JsonArray params = new JsonArray();

        if(newPage) {
          params.add(title).add(markdown);
        } else {
          params.add(markdown).add(id);
        }

        connection.updateWithParams(sql, params, resultHandler -> {
          connection.close();

          if(resultHandler.succeeded()) {
            context.response().setStatusCode(303);
            context.response().putHeader("Location", "/wiki/" + title);
            context.response().end();
          } else {
            context.fail(resultHandler.cause());
          }
        });
      } else {
        context.fail(asyncResult.cause());
      }
    });
  }

  private void pageDeletionHandler(RoutingContext context) {
    String id = context.request().getParam("id");

    dbClient.getConnection(asyncResult -> {
      if(asyncResult.succeeded()) {
        SQLConnection connection = asyncResult.result();
        connection.updateWithParams(SQL_DELETE_PAGE, new JsonArray().add(id), resultHandler -> {
          connection.close();

          if(resultHandler.succeeded()) {
            context.response().setStatusCode(303);
            context.response().putHeader("Location", "/");
            context.response().end();
          } else {
            context.fail(resultHandler.cause());
          }
        });
      } else {
        context.fail(asyncResult.cause());
      }
    });
  }

}
