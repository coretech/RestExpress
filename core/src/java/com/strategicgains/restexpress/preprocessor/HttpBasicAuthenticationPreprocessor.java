/*
    Copyright 2013, Strategic Gains, Inc.

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
 */
package com.strategicgains.restexpress.preprocessor;

import io.netty.handler.codec.http.HttpHeaders;

import javax.xml.bind.DatatypeConverter;

import com.strategicgains.restexpress.Flags;
import com.strategicgains.restexpress.Request;
import com.strategicgains.restexpress.exception.UnauthorizedException;
import com.strategicgains.restexpress.pipeline.Preprocessor;
import com.strategicgains.restexpress.route.Route;

/**
 * This preprocessor implements HTTP Basic authentication. It simply parses the
 * Authorization header, putting the username and password in request headers.
 * To use it, simply add it to your RestExpress server as follows:
 * 
 * <code>
 * server.addPreprocessor(new HttpBasicAuthenticationPreprocessor("my realm"));
 * </code>
 * <p/>
 * Once this preprocessor completes successfully, it places the username and
 * password in the request as headers, X-AuthenticatedUser and
 * X-AuthenticatedPassword, respectively.
 * <p/>
 * If the preprocessor fails, it throws an UnauthorizedException, which results in
 * an HTTP status of 401 to the caller.  It also sets the WWW-Authenticate header
 * to 'Basic realm=<provided realm>' where <provided realm> is the arbitrary realm
 * name passed in on instantiation.
 * <p/>
 * Use of this preprocessor assumes you'll implement an authorization
 * preprocessor that validates the username and password.
 * 
 * @author toddf
 * @since Feb 28, 2013
 */
public class HttpBasicAuthenticationPreprocessor
implements Preprocessor
{
	public static final String X_AUTHENTICATED_USER = "X-AuthenticatedUser";
	public static final String X_AUTHENTICATED_PASSWORD = "X-AuthenticatedPassword";

	private String realm;

	/**
	 * Utilize HTTP Basic Authentication with the given realm returned on an
	 * unauthenticated request.
	 * 
	 * @param realm
	 *            any value to identify the secure area and may used by HTTP
	 *            clients to manage passwords.
	 */
	public HttpBasicAuthenticationPreprocessor(String realm)
	{
		super();
		this.realm = realm;
	}

	@Override
	public void process(Request request)
	{
		Route route = request.getResolvedRoute();

		if (route != null && (route.isFlagged(Flags.Auth.PUBLIC_ROUTE)
			|| route.isFlagged(Flags.Auth.NO_AUTHENTICATION)))
		{
			return;
		}

		String authorization = request.getHeader(HttpHeaders.Names.AUTHORIZATION);

		if (authorization == null || !authorization.startsWith("Basic "))
		{
			throwUnauthorizedException();
		}

		String[] pieces = authorization.split(" ");
		byte[] bytes = DatatypeConverter.parseBase64Binary(pieces[1]);
		String credentials = new String(bytes);
		String[] parts = credentials.split(":");

		if (parts.length < 2)
		{
			throwUnauthorizedException();
		}

		request.addHeader(X_AUTHENTICATED_USER, parts[0]);
		request.addHeader(X_AUTHENTICATED_PASSWORD, parts[1]);
	}

	private void throwUnauthorizedException()
    {
	    UnauthorizedException e = new UnauthorizedException("Authentication required");
	    e.setHeader(HttpHeaders.Names.WWW_AUTHENTICATE, "Basic realm=\"" + realm + "\"");
	    throw e;
    }
}