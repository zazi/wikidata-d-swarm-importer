package org.dswarm.wikidataimporter;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.rx.RxInvocationBuilder;
import org.glassfish.jersey.client.rx.RxWebTarget;
import org.glassfish.jersey.client.rx.rxjava.RxObservable;
import org.glassfish.jersey.client.rx.rxjava.RxObservableInvoker;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.wdtk.datamodel.helpers.DatamodelConverter;
import org.wikidata.wdtk.datamodel.implementation.PropertyDocumentImpl;
import org.wikidata.wdtk.datamodel.interfaces.DataObjectFactory;
import org.wikidata.wdtk.datamodel.interfaces.EntityDocument;
import org.wikidata.wdtk.datamodel.interfaces.EntityIdValue;
import org.wikidata.wdtk.datamodel.interfaces.ItemDocument;
import org.wikidata.wdtk.datamodel.json.jackson.JacksonObjectFactory;
import org.wikidata.wdtk.datamodel.json.jackson.JacksonPropertyDocument;
import rx.Observable;
import rx.schedulers.Schedulers;

/**
 * @author tgaengler
 */
public class WikibaseAPIClient {

	private static final Logger LOG = LoggerFactory.getLogger(WikibaseAPIClient.class);

	private static final Properties properties = new Properties();

	private static final String MEDIAWIKI_API_ENDPOINT          = "mediawiki_api_endpoint";
	private static final String FALLBACK_MEDIAWIKI_API_ENDPOINT = "http://localhost:1234/whoknows";
	private static final String DSWARM_USER_AGENT_IDENTIFIER    = "DMP 2000";
	public static final  String WIKIBASE_API_DSWARM_CLIENT      = "wikibase-api-dswarm-client";
	private static final String MEDIAWIKI_USERNAME              = "mediawiki_username";
	private static final String MEDIAWIKI_PASSWORD              = "mediawiki_password";

	private static final String wikibaseAPIBaseURI;
	private static final int REQUEST_GAP = 50;

	static {

		final URL resource = Resources.getResource("dswarm.properties");

		try {

			properties.load(resource.openStream());
		} catch (final IOException e) {

			LOG.error("Could not load dswarm.properties", e);
		}

		wikibaseAPIBaseURI = properties.getProperty(MEDIAWIKI_API_ENDPOINT, FALLBACK_MEDIAWIKI_API_ENDPOINT);
	}

	private static final String CHUNKED = "CHUNKED";

	private static final int CHUNK_SIZE      = 1024;
	private static final int REQUEST_TIMEOUT = 20000000;

	private static final String          DSWARM_WIKIDATA_GDM_IMPORTER_THREAD_NAMING_PATTERN = "dswarm-wikidata-gdm-importer-%d";
	private static final ExecutorService EXECUTOR_SERVICE                                   = Executors.newCachedThreadPool(
			new BasicThreadFactory.Builder().daemon(false).namingPattern(DSWARM_WIKIDATA_GDM_IMPORTER_THREAD_NAMING_PATTERN).build());

	private static final ClientBuilder BUILDER = ClientBuilder.newBuilder().register(MultiPartFeature.class)
			.property(ClientProperties.CHUNKED_ENCODING_SIZE, CHUNK_SIZE)
			.property(ClientProperties.REQUEST_ENTITY_PROCESSING, CHUNKED)
			.property(ClientProperties.OUTBOUND_CONTENT_LENGTH_BUFFER, CHUNK_SIZE)
			.property(ClientProperties.CONNECT_TIMEOUT, REQUEST_TIMEOUT)
			.property(ClientProperties.READ_TIMEOUT, REQUEST_TIMEOUT);

	private static final String MEDIAWIKI_API_ACTION_IDENTIFIER = "action";

	private static final String MEDIAWIKI_API_FORMAT_IDENTIFIER = "format";

	private static final String MEDIAWIKI_API_LOGIN                 = "login";
	private static final String MEDIAWIKI_API_LGNAME_IDENTIFIER     = "lgname";
	private static final String MEDIAWIKI_API_LGPASSWORD_IDENTIFIER = "lgpassword";
	private static final String MEDIAWIKI_API_LGTOKEN_IDENTIFIER    = "lgtoken";

	private static final String MEDIAWIKI_API_QUERY               = "query";
	private static final String MEDIAWIKI_API_META_IDENTIFIER     = "meta";
	private static final String MEDIAWIKI_API_CONTINUE_IDENTIFIER = "continue";

	private static final String WIKIBASE_API_NEW_IDENTIFIER  = "new";
	private static final String WIKIBASE_API_DATA_IDENTIFIER = "data";

	private static final String MEDIAWIKI_API_TOKEN_IDENTIFIER = "token";

	private static final String MEDIAWIKI_API_JSON_FORMAT          = "json";
	private static final String MEDIAWIKI_API_TOKENS_IDENTIFIER    = "tokens";
	private static final String MEDIAWIKI_API_CSRFTOKEN_IDENTIFIER = "csrftoken";

	public static final String WIKIBASE_API_ENTITY_TYPE_ITEM     = "item";
	public static final String WIKIBASE_API_ENTITY_TYPE_PROPERTY = "property";

	private static final String WIKIBASE_API_EDIT_ENTITY = "wbeditentity";

	private static final ObjectMapper MAPPER = new ObjectMapper()
			.setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
			.setSerializationInclusion(JsonInclude.Include.NON_NULL);

	private static final DataObjectFactory  jsonOjbectFactory  = new JacksonObjectFactory();
	private static final DatamodelConverter datamodelConverter = new DatamodelConverter(jsonOjbectFactory);

	private final String                 editToken;
	private final Map<String, NewCookie> cookies;

	private final AtomicLong requestCount              = new AtomicLong();
	private final AtomicLong bigRequestCount           = new AtomicLong();
	private final AtomicLong successfulRequestCount    = new AtomicLong();
	private final AtomicLong bigSuccessfulRequestCount = new AtomicLong();
	private final AtomicLong failedRequestCount        = new AtomicLong();
	private final AtomicLong bigFailedRequestCount     = new AtomicLong();

	public WikibaseAPIClient() throws WikidataImporterException {

		final Map<String, Map<String, NewCookie>> result = generateEditToken();

		if (result == null) {

			final String message = "couldn't generate edit token successfully - API cannot be utilised for edit requests";

			LOG.error(message);

			throw new WikidataImporterException(message);
		}

		final Map.Entry<String, Map<String, NewCookie>> resultEntry = result.entrySet().iterator().next();

		editToken = resultEntry.getKey();
		cookies = resultEntry.getValue();
	}

	private Map<String, Map<String, NewCookie>> generateEditToken() {

		LOG.debug("try to generate edit token");

		// 0. read user name + password from properties
		final String username = getProperty(MEDIAWIKI_USERNAME);
		final String password = getProperty(MEDIAWIKI_PASSWORD);

		// 1. login request
		return login(username, password).flatMap(loginResponse -> {

			// 1.1 get token from login request response
			final String token = getToken(loginResponse);

			// 1.2 get cookies from login request response
			final Map<String, NewCookie> loginRequestCookies = getCookies(loginResponse);

			if (token == null || loginRequestCookies == null) {

				LOG.error("couldn't retrieve token successfully - cannot continue edit token generation");

				return Observable.empty();
			}

			LOG.debug("retrieved token with login credentials successfully");

			// 2. confirm login request
			return confirmLogin(token, loginRequestCookies).flatMap(confirmLoginResponse -> {

				// 2.1 get cookies from login confirm response
				final Map<String, NewCookie> confirmLoginCookies = getCookies(confirmLoginResponse);

				if (confirmLoginCookies == null) {

					LOG.error("couldn't confirm login token successfully - cannot continue edit token generation");

					return Observable.empty();
				}

				LOG.debug("confirmed login with token and cookies successfully");

				// 3. retrieve edit token request
				return retrieveEditToken(confirmLoginCookies).map(retrieveEditTokenResponse -> {

					// 3.1 get edit token from edit token response
					final String editToken = getEditToken(retrieveEditTokenResponse);

					// 3.2 get cookies from edit token response
					final Map<String, NewCookie> editTokenCookies = getCookies(retrieveEditTokenResponse);

					if (editTokenCookies == null) {

						LOG.error("couldn't retrieve edit token successfully - cannot continue edit token generation");

						return null;
					}

					LOG.debug("retrieved edit token with cookies successfully");

					// 3.3 merge cookies from response from 2 + 3
					loginRequestCookies.putAll(editTokenCookies);

					final Map<String, Map<String, NewCookie>> result = new HashMap<>();
					result.put(editToken, loginRequestCookies);

					LOG.debug("generated edit token successfully");

					return result;
				});
			});
		}).toBlocking().firstOrDefault(null);
	}

	public static Observable<Response> login(final String username, final String password) {

		LOG.debug("try to retrieve token with login credentials");

		final RxWebTarget<RxObservableInvoker> rxWebTarget = rxWebTarget();

		final RxObservableInvoker rx = rxWebTarget.request()
				.header(HttpHeaders.USER_AGENT, DSWARM_USER_AGENT_IDENTIFIER)
				.rx();

		final FormDataMultiPart form = new FormDataMultiPart()
				.field(MEDIAWIKI_API_ACTION_IDENTIFIER, MEDIAWIKI_API_LOGIN)
				.field(MEDIAWIKI_API_LGNAME_IDENTIFIER, username)
				.field(MEDIAWIKI_API_LGPASSWORD_IDENTIFIER, password)
				.field(MEDIAWIKI_API_FORMAT_IDENTIFIER, MEDIAWIKI_API_JSON_FORMAT);

		return excutePOST(rx, form);
	}

	public static Observable<Response> confirmLogin(final String token, final Map<String, NewCookie> cookies) {

		LOG.debug("try to confirm login with token and cookies");

		final RxObservableInvoker rx = buildBaseRequestWithCookies(cookies);

		final FormDataMultiPart form = new FormDataMultiPart()
				.field(MEDIAWIKI_API_ACTION_IDENTIFIER, MEDIAWIKI_API_LOGIN)
				.field(MEDIAWIKI_API_LGTOKEN_IDENTIFIER, token);

		return excutePOST(rx, form);
	}

	public static Observable<Response> retrieveEditToken(final Map<String, NewCookie> cookies) {

		LOG.debug("try to retrieve edit token with cookies");

		final RxObservableInvoker rx = buildBaseRequestWithCookies(cookies);

		final FormDataMultiPart form = new FormDataMultiPart()
				.field(MEDIAWIKI_API_ACTION_IDENTIFIER, MEDIAWIKI_API_QUERY)
				.field(MEDIAWIKI_API_META_IDENTIFIER, MEDIAWIKI_API_TOKENS_IDENTIFIER)
				.field(MEDIAWIKI_API_CONTINUE_IDENTIFIER, "")
				.field(MEDIAWIKI_API_FORMAT_IDENTIFIER, MEDIAWIKI_API_JSON_FORMAT);

		return excutePOST(rx, form);
	}

	public static String getToken(final Response loginResponse) {

		try {

			final String responseBody = loginResponse.readEntity(String.class);

			if (responseBody == null) {

				LOG.error("cannot extract token - response body is not available");

				return null;
			}

			final ObjectNode json = MAPPER.readValue(responseBody, ObjectNode.class);

			if (json == null) {

				LOG.error("cannot extract token - response JSON is not available");

				return null;
			}

			final JsonNode loginNode = json.get(MEDIAWIKI_API_LOGIN);

			if (loginNode == null) {

				LOG.error("cannot extract token - '{}' node is not available in response JSON '{}'", MEDIAWIKI_API_LOGIN, responseBody);

				return null;
			}

			final JsonNode tokenNode = loginNode.get(MEDIAWIKI_API_TOKEN_IDENTIFIER);

			if (tokenNode == null) {

				LOG.error("cannot extract token - '{}' node is not available in response JSON '{}'", MEDIAWIKI_API_TOKEN_IDENTIFIER, responseBody);

				return null;
			}

			return tokenNode.asText();
		} catch (final Exception e) {

			LOG.error("cannot extract token - an error occurred while trying to extract the token from the response body", e);

			return null;
		}
	}

	public static String getEditToken(final Response editTokenResponse) {

		try {

			final String responseBody = editTokenResponse.readEntity(String.class);

			if (responseBody == null) {

				LOG.error("cannot extract edit token - response body is not available");

				return null;
			}

			final ObjectNode json = MAPPER.readValue(responseBody, ObjectNode.class);

			if (json == null) {

				LOG.error("cannot extract edit token - response JSON is not available");

				return null;
			}

			final JsonNode queryNode = json.get(MEDIAWIKI_API_QUERY);

			if (queryNode == null) {

				LOG.error("cannot extract edit token - '{}' node is not available in response JSON '{}'", MEDIAWIKI_API_QUERY, responseBody);

				return null;
			}

			final JsonNode tokensNode = queryNode.get(MEDIAWIKI_API_TOKENS_IDENTIFIER);

			if (tokensNode == null) {

				LOG.error("cannot extract edit token - '{}' node is not available in response JSON '{}'", MEDIAWIKI_API_TOKENS_IDENTIFIER,
						responseBody);

				return null;
			}

			final JsonNode csrfTokenNode = tokensNode.get(MEDIAWIKI_API_CSRFTOKEN_IDENTIFIER);

			if (csrfTokenNode == null) {

				LOG.error("cannot extract edit token - '{}' node is not available in response JSON '{}'", MEDIAWIKI_API_CSRFTOKEN_IDENTIFIER,
						responseBody);

				return null;
			}

			return csrfTokenNode.asText();
		} catch (final Exception e) {

			LOG.error("cannot extract edit token - an error occurred while trying to extract the edit token from the response body", e);

			return null;
		}
	}

	public Observable<Response> createEntity(final EntityDocument entity, final String entityType)
			throws JsonProcessingException, WikidataImporterException {

		//		final EntityDocument jacksonEntity;
		//
		//		switch (entityType) {
		//
		//			case WIKIBASE_API_ENTITY_TYPE_ITEM:
		//
		//				//jacksonEntity = JacksonItemDocument.fromItemDocumentImpl((ItemDocumentImpl) entity);
		//				jacksonEntity = datamodelConverter.copy((ItemDocument) entity);
		//
		//				break;
		//			case WIKIBASE_API_ENTITY_TYPE_PROPERTY:
		//
		//				jacksonEntity = JacksonPropertyDocument.fromPropertyDocumentImpl((PropertyDocumentImpl) entity);
		//
		//				break;
		//			default:
		//
		//				final String message = String.format("unknown entity type '%s'", entityType);
		//
		//				LOG.error(message);
		//
		//				throw new WikidataImporterException(message);
		//		}
		//
		//		final String entityJSONString = MAPPER.writeValueAsString(jacksonEntity);
		//
		//		LOG.debug("create new '{}' with '{}'", entityType, entityJSONString);
		//
		//		final RxObservableInvoker rx = buildBaseRequestWithCookies(cookies);
		//
		//		final FormDataMultiPart form = new FormDataMultiPart()
		//				.field(MEDIAWIKI_API_ACTION_IDENTIFIER, WIKIBASE_API_EDIT_ENTITY)
		//				.field(WIKIBASE_API_NEW_IDENTIFIER, entityType)
		//				.field(WIKIBASE_API_DATA_IDENTIFIER, entityJSONString)
		//				.field(MEDIAWIKI_API_TOKEN_IDENTIFIER, editToken)
		//				.field(MEDIAWIKI_API_FORMAT_IDENTIFIER, MEDIAWIKI_API_JSON_FORMAT);
		//		//form.bodyPart(entityJSONString, MediaType.APPLICATION_JSON_TYPE);
		//
		//		return excutePOST(rx, form);

		//return new CreateEntityCommand(entity, entityType).toObservable();

		return new CreateEntityRequestOperator(entityType).processEntity(entity);
	}

	public static Map<String, NewCookie> getCookies(final Response response) {

		return response.getCookies();
	}

	private static RxObservableInvoker buildBaseRequestWithCookies(final Map<String, NewCookie> cookies) {

		final RxWebTarget<RxObservableInvoker> rxWebTarget = rxWebTarget();

		RxInvocationBuilder<RxObservableInvoker> request = rxWebTarget.request()
				.header(HttpHeaders.USER_AGENT, DSWARM_USER_AGENT_IDENTIFIER);

		if (cookies != null) {

			for (final Cookie cookie : cookies.values()) {

				request = request.cookie(cookie);
			}
		}

		return request.rx();
	}

	private static Observable<Response> excutePOST(final RxObservableInvoker rx, final FormDataMultiPart form) {

		final Entity entityBody = Entity.entity(form, MediaType.MULTIPART_FORM_DATA);

		final Observable<Response> post = Observable.timer(REQUEST_GAP, TimeUnit.MILLISECONDS).flatMap(
				aLong -> rx.post(entityBody).onBackpressureBuffer().subscribeOn(Schedulers.from(EXECUTOR_SERVICE)));

		return post.filter(response -> {

			if (response == null) {

				LOG.error("response was null, discontinue processing");

				return false;
			}

			final int status = response.getStatus();

			return status == 200;
		});
	}

	private static Client client() {

		return BUILDER.build();
	}

	private static WebTarget target() {

		return client().target(wikibaseAPIBaseURI);
	}

	private static WebTarget target(final String... path) {

		WebTarget target = target();

		for (final String p : path) {

			target = target.path(p);
		}

		return target;
	}

	private static RxWebTarget<RxObservableInvoker> rxWebTarget() {

		final WebTarget target = target();

		return RxObservable.from(target);
	}

	private static RxWebTarget<RxObservableInvoker> rxWebTarget(final String... path) {

		final WebTarget target = target(path);

		return RxObservable.from(target);
	}

	private static String getProperty(final String propertyKey) {

		final String propertyValue = properties.getProperty(propertyKey);

		if (propertyValue == null || propertyValue.trim().isEmpty()) {

			LOG.error("couldn't find property '{}' in properties file", propertyKey);
		}

		return propertyValue;
	}

	//	//
	//	// class CreateEntityCommand
	//	//
	//	private class CreateEntityCommand extends CommonCommand<Response> {
	//
	//		private static final int  RESUME_LIMIT        = 10;
	//		private static final long RETRY_REQUEST_DELAY = 1000;
	//
	//		private final EntityDocument entity;
	//		private final String         entityType;
	//
	//		private final AtomicInteger retryCount;
	//		private final int           resumeLimit;
	//
	//		public CreateEntityCommand(final EntityDocument entityArg, final String entityTypeArg) {
	//
	//			// REQUEST_TIMEOUT
	//			super("destination=" + WIKIBASE_API_EDIT_ENTITY,
	//					RESUME_LIMIT * (int) RETRY_REQUEST_DELAY * 10 + RESUME_LIMIT * (int) RETRY_REQUEST_DELAY);
	//
	//			entity = entityArg;
	//			entityType = entityTypeArg;
	//			retryCount = new AtomicInteger();
	//			resumeLimit = RESUME_LIMIT;
	//		}
	//
	//		public CreateEntityCommand(final EntityDocument entityArg, final String entityTypeArg, final AtomicInteger retryCountArg) {
	//
	//			super("destination=" + WIKIBASE_API_EDIT_ENTITY, REQUEST_TIMEOUT);
	//
	//			entity = entityArg;
	//			entityType = entityTypeArg;
	//			retryCount = retryCountArg;
	//			resumeLimit = RESUME_LIMIT;
	//		}
	//
	//		@Override
	//		protected Observable<Response> construct() {
	//
	//			try {
	//
	//				final EntityDocument jacksonEntity;
	//
	//				switch (entityType) {
	//
	//					case WIKIBASE_API_ENTITY_TYPE_ITEM:
	//
	//						//jacksonEntity = JacksonItemDocument.fromItemDocumentImpl((ItemDocumentImpl) entity);
	//						jacksonEntity = datamodelConverter.copy((ItemDocument) entity);
	//
	//						break;
	//					case WIKIBASE_API_ENTITY_TYPE_PROPERTY:
	//
	//						jacksonEntity = JacksonPropertyDocument.fromPropertyDocumentImpl((PropertyDocumentImpl) entity);
	//
	//						break;
	//					default:
	//
	//						final String message = String.format("unknown entity type '%s'", entityType);
	//
	//						LOG.error(message);
	//
	//						throw new WikidataImporterException(message);
	//				}
	//
	//				final String entityJSONString = MAPPER.writeValueAsString(jacksonEntity);
	//
	//				LOG.debug("create new '{}' with '{}'", entityType, entityJSONString);
	//
	//				final RxObservableInvoker rx = buildBaseRequestWithCookies(cookies);
	//
	//				final FormDataMultiPart form = new FormDataMultiPart()
	//						.field(MEDIAWIKI_API_ACTION_IDENTIFIER, WIKIBASE_API_EDIT_ENTITY)
	//						.field(WIKIBASE_API_NEW_IDENTIFIER, entityType)
	//						.field(WIKIBASE_API_DATA_IDENTIFIER, entityJSONString)
	//						.field(MEDIAWIKI_API_TOKEN_IDENTIFIER, editToken)
	//						.field(MEDIAWIKI_API_FORMAT_IDENTIFIER, MEDIAWIKI_API_JSON_FORMAT);
	//				//form.bodyPart(entityJSONString, MediaType.APPLICATION_JSON_TYPE);
	//
	//				// simulate retry behaviour here
	//				// ).onExceptionResumeNext(
	//				return excutePOST(rx, form).observeOn(Schedulers.io()).onErrorResumeNext(ex -> {
	//					LOG.error("in onErrorResumeNext entity type '{}'",
	//							entityType, ex);
	//					return Observable.timer(RETRY_REQUEST_DELAY, TimeUnit.MILLISECONDS).take(1)
	//							.filter(aLong -> {
	//
	//								final String entityId = determineEntityId();
	//
	//								LOG.debug("retry count '{}' :: resume limit '{}' entity type '{}'", retryCount.get(), resumeLimit, entityType);
	//
	//								if (retryCount.get() <= resumeLimit) {
	//
	//									LOG.debug("retry create-entity request for '{}' of entity type '{}' for the '{}' time",
	//											entityId, entityType,
	//											retryCount.get());
	//
	//									retryCount.incrementAndGet();
	//
	//									return true;
	//								}
	//
	//								LOG.debug("reached limit for create-entity request of '{}' of entity type '{}'", entityId,
	//										entityType);
	//
	//								return false;
	//							}).flatMap(aLong -> new CreateEntityCommand(entity, entityType, retryCount)
	//									.toObservable());
	//				});
	//			} catch (final WikidataImporterException e) {
	//
	//				throw WikidataImporterError.wrap(e);
	//			} catch (final Exception e) {
	//
	//				final String message = String.format("could not create entity of entity type '%s'", entityType);
	//
	//				LOG.error(message, e);
	//
	//				throw WikidataImporterError.wrap(new WikidataImporterException(message, e));
	//			}
	//		}
	//
	//		@Override
	//		protected Observable<Response> resumeWithFallback() {
	//
	//			handleErrors();
	//
	//			// inspired by http://stackoverflow.com/a/27094967/1022591
	//			// note: we always need to fire a new hystrix command, because this requires new network traffic, see
	//			//   This should do work that does not require network transport to produce.
	//			//   In other words, this should be a static or cached result that can immediately be returned upon failure.
	//			//   If network traffic is wanted for fallback (such as going to MemCache) then the fallback implementation should invoke another HystrixObservableCommand instance that protects against that network access and possibly has another level of fallback that does not involve network access.
	//			// from https://netflix.github.io/Hystrix/javadoc/com/netflix/hystrix/HystrixObservableCommand.html#construct%28%29
	//
	//			// do retry logic in construct, since it looks like that we cannot do any more network calls from here, see AbstractCommand#getFallbackOrThrowException
	//			//   If something in the <code>getFallback()</code> implementation is latent (such as a network call) then the semaphore will cause us to start rejecting requests rather than allowing potentially
	//			//   all threads to pile up and block.
	//			//
	//			//			if (retryCount.get() <= RESUME_LIMIT) {
	//			//
	//			//				retryCount.incrementAndGet();
	//			//
	//			//				LOG.debug("retry create-entity request for '{}' of entity type '{}' for the '{}' time", entity.getEntityId().getId(), entityType,
	//			//						retryCount.get());
	//			//
	//			//				// return new hystrix command observable delayed
	//			//				//				return new CreateEntityCommand(entity, entityType, retryCount).construct()
	//			//				//						.retryWhen(delay -> Observable.timer(RETRY_REQUEST_DELAY, TimeUnit.MILLISECONDS));
	//			//				//				return new CreateEntityCommand(entity, entityType, retryCount).construct().observeOn(Schedulers.io())
	//			//				//						.delay(delay -> Observable.timer(RETRY_REQUEST_DELAY, TimeUnit.MILLISECONDS));
	//			//				return Observable.timer(RETRY_REQUEST_DELAY, TimeUnit.MILLISECONDS)
	//			//						.flatMap(aLong -> new CreateEntityCommand(entity, entityType, retryCount).toObservable().subscribeOn(Schedulers.newThread()))
	//			//						.take(1);
	//			//			}
	//
	//			final String entityId = determineEntityId();
	//
	//			LOG.debug("reached limit for create-entity request of '{}' of entity type '{}'", entityId, entityType);
	//
	//			return Observable.empty();
	//
	//			//			return construct().retryWhen(
	//			//					attempts -> attempts.zipWith(Observable.range(1, RESUME_LIMIT + 1),
	//			//							(Func2<Throwable, Integer, Tuple<Throwable, Integer>>) Tuple::new)
	//			//							.flatMap(
	//			//									ni -> {
	//			//
	//			//										final Integer attempt = ni.v2();
	//			//
	//			//										if (attempt > RESUME_LIMIT) {
	//			//
	//			//											final Throwable exception = ni.v1();
	//			//
	//			//											LOG.error("something happened", exception);
	//			//
	//			//											// shall we return an empty observable here instead, and log that request (attempts) failed for some reason?
	//			//											return Observable.error(exception);
	//			//										}
	//			//
	//			//										// (long) Math.pow(2, ni.v2())
	//			//										return Observable.timer(RETRY_REQUEST_DELAY, TimeUnit.MILLISECONDS);
	//			//									}));
	//		}
	//
	//		private String determineEntityId() {
	//
	//			if (entity == null) {
	//
	//				LOG.debug("cannot determine entity id - entity is not available");
	//
	//				return null;
	//			}
	//
	//			final EntityIdValue entityId = entity.getEntityId();
	//
	//			if (entityId == null) {
	//
	//				LOG.debug("cannot determine entity id - entity id object is not available");
	//
	//				return null;
	//			}
	//
	//			return entityId.getId();
	//		}
	//	} //class CreateEntityCommand

	private class CreateEntityRequestOperator {

		private static final int  RESUME_LIMIT        = 10;
		private static final long RETRY_REQUEST_DELAY = 1000;

		private final String entityType;

		private final AtomicInteger retryCount;
		private final int           resumeLimit;

		public CreateEntityRequestOperator(final String entityTypeArg) {

			// REQUEST_TIMEOUT
			//			super("destination=" + WIKIBASE_API_EDIT_ENTITY,
			//					RESUME_LIMIT * (int) RETRY_REQUEST_DELAY * 10 + RESUME_LIMIT * (int) RETRY_REQUEST_DELAY);

			entityType = entityTypeArg;
			retryCount = new AtomicInteger();
			resumeLimit = RESUME_LIMIT;
		}

		public CreateEntityRequestOperator(final String entityTypeArg, final AtomicInteger retryCountArg) {

			//			super("destination=" + WIKIBASE_API_EDIT_ENTITY, REQUEST_TIMEOUT);

			entityType = entityTypeArg;
			retryCount = retryCountArg;
			resumeLimit = RESUME_LIMIT;
		}

		public Observable<Response> processEntity(final EntityDocument entity) {

			final long currentRequestCount = requestCount.incrementAndGet();

			if (currentRequestCount / 10000 == bigRequestCount.get()) {

				bigRequestCount.incrementAndGet();

				LOG.info("started '{}' requests", currentRequestCount);
			}

			try {

				final EntityDocument jacksonEntity;

				switch (entityType) {

					case WIKIBASE_API_ENTITY_TYPE_ITEM:

						//jacksonEntity = JacksonItemDocument.fromItemDocumentImpl((ItemDocumentImpl) entity);
						jacksonEntity = datamodelConverter.copy((ItemDocument) entity);

						break;
					case WIKIBASE_API_ENTITY_TYPE_PROPERTY:

						jacksonEntity = JacksonPropertyDocument.fromPropertyDocumentImpl((PropertyDocumentImpl) entity);

						break;
					default:

						final String message = String.format("unknown entity type '%s'", entityType);

						LOG.error(message);

						throw new WikidataImporterException(message);
				}

				final String entityJSONString = MAPPER.writeValueAsString(jacksonEntity);

				LOG.debug("create new '{}' with '{}'", entityType, entityJSONString);

				final RxObservableInvoker rx = buildBaseRequestWithCookies(cookies);

				final FormDataMultiPart form = new FormDataMultiPart()
						.field(MEDIAWIKI_API_ACTION_IDENTIFIER, WIKIBASE_API_EDIT_ENTITY)
						.field(WIKIBASE_API_NEW_IDENTIFIER, entityType)
						.field(WIKIBASE_API_DATA_IDENTIFIER, entityJSONString)
						.field(MEDIAWIKI_API_TOKEN_IDENTIFIER, editToken)
						.field(MEDIAWIKI_API_FORMAT_IDENTIFIER, MEDIAWIKI_API_JSON_FORMAT);

				// simulate retry behaviour here
				// ).onExceptionResumeNext(
				// TODO: we probably need another scheduler here, or? - e.g. a thread pool with a fixed size
				return excutePOST(rx, form).observeOn(Schedulers.io()).onErrorResumeNext(ex -> {

					final long currentFailedRequestCount = failedRequestCount.incrementAndGet();

					if (currentFailedRequestCount / 10000 == bigFailedRequestCount.get()) {

						bigFailedRequestCount.incrementAndGet();

						LOG.info("failed '{}' requests (from '{}' started requests)", currentFailedRequestCount, requestCount.get());
					}

					//LOG.error("in onErrorResumeNext with entity type '{}'", entityType);

					LOG.trace("in onErrorResumeNext with entity type '{}'", entityType, ex);

					return Observable.timer(RETRY_REQUEST_DELAY, TimeUnit.MILLISECONDS)
							.filter(aLong -> {

								final String entityId = determineEntityId(entity);

								final int retryCount = this.retryCount.incrementAndGet();

								LOG.debug("retry count '{}' :: resume limit '{}' entity type '{}'", retryCount, resumeLimit, entityType);

								if (retryCount <= resumeLimit) {

									LOG.debug("retry create-entity request for '{}' of entity type '{}' for the '{}' time", entityId, entityType,
											retryCount);

									return true;
								}

								LOG.debug("reached limit for create-entity request of '{}' of entity type '{}'", entityId, entityType);

								return false;
							}).flatMap(aLong -> processEntity(entity)).doOnCompleted(() -> {

								final long currentSuccessfulRequestCount = successfulRequestCount.incrementAndGet();

								if (currentSuccessfulRequestCount / 10000 == bigSuccessfulRequestCount.get()) {

									bigSuccessfulRequestCount.incrementAndGet();

									LOG.info("processed '{}' requests successfully (from '{}' started requests)", currentSuccessfulRequestCount,
											requestCount.get());
								}
							});
				});
			} catch (final WikidataImporterException e) {

				throw WikidataImporterError.wrap(e);
			} catch (final Exception e) {

				final String message = String.format("could not create entity of entity type '%s'", entityType);

				LOG.error(message, e);

				throw WikidataImporterError.wrap(new WikidataImporterException(message, e));
			}
		}

		private String determineEntityId(final EntityDocument entity) {

			if (entity == null) {

				LOG.debug("cannot determine entity id - entity is not available");

				return null;
			}

			final EntityIdValue entityId = entity.getEntityId();

			if (entityId == null) {

				LOG.debug("cannot determine entity id - entity id object is not available");

				return null;
			}

			return entityId.getId();
		}
	}
}
