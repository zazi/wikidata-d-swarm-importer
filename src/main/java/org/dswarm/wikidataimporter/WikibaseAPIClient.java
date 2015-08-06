package org.dswarm.wikidataimporter;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;

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
import org.wikidata.wdtk.datamodel.interfaces.EntityDocument;
import rx.Observable;
import rx.schedulers.Schedulers;

/**
 * @author tgaengler
 */
public class WikibaseAPIClient {

	private static final Logger LOG = LoggerFactory.getLogger(WikibaseAPIClient.class);

	private static final String wikibaseAPIBaseURI;
	public static final String DSWARM_USER_AGENT_IDENTIFIER = "DMP 2000";

	static {

		final URL resource = Resources.getResource("dswarm.properties");
		final Properties properties = new Properties();

		try {

			properties.load(resource.openStream());
		} catch (final IOException e) {

			LOG.error("Could not load dswarm.properties", e);
		}

		wikibaseAPIBaseURI = properties.getProperty("wikibase_api_endpoint", "http://localhost:1234/whoknows");
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
	private static final String WIKIBASE_API_ITEM_IDENTIFIER       = "item";

	private static final String WIKIBASE_API_EDIT_ENTITY = "wbeditentity";

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private final String editToken;

	public WikibaseAPIClient() {

		editToken = generateEditToken();
	}

	private String generateEditToken() {

		// 0. read user name + password from properties
		// 1. login request
		// 1.1 get token from login request response
		// 1.2 get cookies from login request response
		// 2. confirm login request
		// 2.1 get cookies from login confirm response
		// 3. retrieve edit token request
		// 3.1 get edit token from edit token response
		// 3.2 get cookies from edit token response
		// 3.3 merge cookies from response from 2 + 3

		// return edit token + cookies

		return null;
	}

	public static Observable<Response> login(final String username, final String password) {

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

		final RxObservableInvoker rx = buildBaseRequestWithCookies(cookies);

		final FormDataMultiPart form = new FormDataMultiPart()
				.field(MEDIAWIKI_API_ACTION_IDENTIFIER, MEDIAWIKI_API_LOGIN)
				.field(MEDIAWIKI_API_LGTOKEN_IDENTIFIER, token);

		return excutePOST(rx, form);
	}

	public static Observable<Response> retrieveEditToken(final Map<String, NewCookie> cookies) {

		final RxObservableInvoker rx = buildBaseRequestWithCookies(cookies);

		final FormDataMultiPart form = new FormDataMultiPart()
				.field(MEDIAWIKI_API_ACTION_IDENTIFIER, MEDIAWIKI_API_QUERY)
				.field(MEDIAWIKI_API_META_IDENTIFIER, MEDIAWIKI_API_TOKENS_IDENTIFIER)
				.field(MEDIAWIKI_API_CONTINUE_IDENTIFIER, "");

		return excutePOST(rx, form);
	}

	public static Observable<String> getToken(final Observable<Response> loginResponse) {

		return loginResponse.map(response1 -> {

			try {

				final String responseBody = response1.readEntity(String.class);

				// TODO null check

				final ObjectNode json = MAPPER.readValue(responseBody, ObjectNode.class);

				// TODO null check

				final JsonNode loginNode = json.get(MEDIAWIKI_API_LOGIN);

				// TODO null check

				final JsonNode tokenNode = loginNode.get(MEDIAWIKI_API_TOKEN_IDENTIFIER);

				// TODO null check

				return tokenNode.asText();
			} catch (final Exception e) {

				e.printStackTrace();

				// TODO wrap/delegate error

				return null;
			}
		});
	}

	public static Observable<String> getEditToken(final Observable<Response> editTokenResponse) {

		return editTokenResponse.map(response -> {

			try {

				final String responseBody = response.readEntity(String.class);

				// TODO null check

				final ObjectNode json = MAPPER.readValue(responseBody, ObjectNode.class);

				// TODO null check

				final JsonNode queryNode = json.get(MEDIAWIKI_API_QUERY);

				// TODO null check

				final JsonNode tokensNode = queryNode.get(MEDIAWIKI_API_TOKENS_IDENTIFIER);

				// TODO null check

				final JsonNode csrfTokenNode = tokensNode.get(MEDIAWIKI_API_CSRFTOKEN_IDENTIFIER);

				// TODO null check

				return csrfTokenNode.asText();
			} catch (final Exception e) {

				e.printStackTrace();

				// TODO wrap/delegate error

				return null;
			}
		});
	}

	public static Observable<Response> createEntity(final EntityDocument entity, final String token, final Map<String, NewCookie> cookies)
			throws JsonProcessingException {

		final String entityJSONString = MAPPER.writeValueAsString(entity);

		final RxObservableInvoker rx = buildBaseRequestWithCookies(cookies);

		final FormDataMultiPart form = new FormDataMultiPart()
				.field(MEDIAWIKI_API_ACTION_IDENTIFIER, WIKIBASE_API_EDIT_ENTITY)
				.field(WIKIBASE_API_NEW_IDENTIFIER, WIKIBASE_API_ITEM_IDENTIFIER)
				.field(WIKIBASE_API_DATA_IDENTIFIER, entityJSONString)
				.field(MEDIAWIKI_API_TOKEN_IDENTIFIER, token);
		//form.bodyPart(entityJSONString, MediaType.APPLICATION_JSON_TYPE);

		return excutePOST(rx, form);
	}

	public static Observable<Map<String, NewCookie>> getCookie(final Observable<Response> response) {

		return response.map(Response::getCookies);
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

		final Observable<Response> post = rx.post(entityBody).subscribeOn(Schedulers.from(EXECUTOR_SERVICE));

		return post.filter(response -> {

			if (response == null) {

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
}