import requests
from flask import current_app as app
from requests.cookies import cookiejar_from_dict, merge_cookies
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util import Retry
from requests.exceptions import MissingSchema, URLRequired
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from urllib.parse import urlparse, urljoin, quote_plus as urlquote
from ast import literal_eval
from functools import wraps
from copy import deepcopy
from getpass import getpass


DEFAULT_TIMEOUT = (5.01, 32)
DEFAULT_RETRIES = 3
DEFAULT_MAX_RETRIES = 5
DEFAULT_BACKOFF_FACTOR = 0.3
DEFAULT_RETRY_CODES = [409, 500, 502, 503, 504, 598, 599]


def int_tuple_eval(val):
    """Returns an int or tuple"""

    try:
        _val = literal_eval(val)
    except ValueError:
        _val = val

    if isinstance(_val, (tuple, int)):
        return _val
    elif isinstance(_val, list):
        return tuple(_val)
    else:
        raise ValueError


def print_request(r, *args, **kwargs):
    """Event hook to log requests, hooks must accept *args and **kwargs"""
    app.logger.info(f"{r.status_code}: [{r.request.method}] {r.url}")


def append_hooks(base: dict, other: dict) -> dict:  # TODO: look at register hooks
    """Resolves a bug in requests.Session: session hooks and request hooks
    do not merge as expected/documented.

    Event hooks should be assigned according to the Requests documentation.
    Those defined on the Connection will persist across the session; those
    defined on a request will be merged with the session hooks for that single
    request only. Defining the same event hook on both the session and request
    will result in duplicate execution of the provided function.

    See requests.hooks for a list of events which can execute a hook (currently only `response`)

    :param base: Primary event hooks dictionary, typically from the session (e.g., {'response': [<some_function>]} )
    :param other: New event hooks dictionary to append, typically from the request
    :return the merged list of hooks for each event.
    """
    for k in other:
        try:
            base[k].append(other[k]) if not isinstance(other[k], list) else base[k].extend(other[k])
        except KeyError:
            base[k] = [other[k]] if not isinstance(other[k], list) else other[k]
        except AttributeError:
            base[k] = [base[k], other[k]] if not isinstance(other[k], list) else other[k].extend(base[k])

    return base


class Connection(object):
    """Provides initialization functionality to requests.Session with a few
    logical improvements:
        * Maintains a base url from the provided url parameter (i.e., https://hostname/api/v1/resource/)
        * Executes requests to an endpoint of the base url using :mod: urllib.parse.urljoin
        * Will ignore the base url if an absolute path is passed as the endpoint (i.e., https://otherhost.com/resource)
        * Provides proper persistence for event hooks across the session and each request
        * Maintains a default request timeout across the session, can be overwritten on each request
        * Ability to set session-wide error and response handling.

    Review urljoin behavior to ensure requests are made to the desired url. For example a `sub-resource/identifier`
    endpoint request will be sent to `https://hostname/api/v1/resource/sub-resource/identifier`, whereas
    `/sub-resource/identifier` will be sent to `https://hostname/api/v1/sub-resource/identifier` given the base url
    example above. Also base urls without a trailing `/` may not function as intended.

        :tldr Beware of your leading and trailing `/`

    :param url: the base url to which all endpoint request should be appended throughout the session (see note above)
    :param auth: a requests.auth.AuthBase object for managing the authentication header across the session
    :param kwargs: (optional) any other attribute(s) to be set on requests.Session
    """

    def __init__(self, url: str, auth, **kwargs):

        if url is not None and not urlparse(url).scheme:
            raise MissingSchema("Server URL must provide a connection scheme e.g., https://hostname")

        self.base_url = url
        self.session = requests.Session()
        self.session.auth = auth
        self.session.verify = kwargs.pop('verify', True)
        self.session.headers.update(kwargs.pop('headers', {}))
        self.session.params.update(kwargs.pop('params', {}))
        self.session.cookies = cookiejar_from_dict(kwargs.pop('cookies', {}))
        self.session.proxies.update(kwargs.pop('proxies', {}))
        self.hooks = append_hooks(self.session.hooks, kwargs.pop('hooks', {}))
        self.timeout = kwargs.pop('timeout', DEFAULT_TIMEOUT)
        self.adapter = kwargs.pop('adapter', None)
        self.logout = kwargs.pop('logout', None)

        if self.adapter:
            self.session.mount('https://', self.adapter)
            self.session.mount('http://', self.adapter)

        [setattr(self.session, attr, value) for attr, value in kwargs.items()]

        if not self.session.verify:
            try:
                requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

            except Exception:
                pass

    def http_request(self, method, endpoint, **kwargs):
        url = urljoin(self.base_url, endpoint) if not urlparse(endpoint).scheme else endpoint
        kwargs['hooks'] = append_hooks(self.hooks, kwargs.pop('hooks', {}))
        kwargs.setdefault('timeout', self.timeout)

        try:
            r = self.session.request(method, url, **kwargs)

        except Exception as e:
            self.error_handler(e, method=method, endpoint=endpoint, **kwargs)

        else:
            return self.response_handler(r, method=method, endpoint=endpoint, **kwargs)

    def error_handler(self, e, **kwargs):
        """
        Overwrite method in subclass to handle errors. Please note, status codes
        should be processed with a response handler not an error handler.
        """
        raise e

    def response_handler(self, r, **kwargs):
        """
        Overwrite method in subclass to process response object prior to return.
        Please note, status codes should be processed with a response handler not
        an error handler. Exceptions raised during response handling will not be
        caught by the error handler. Consider using event hooks if more granular, 
        request-specific response handling is needed.
        """
        return r

    def close(self):
        self.logout()
        self.session.close()

    def get(self, endpoint: str, **kwargs):
        kwargs.setdefault('allow_redirects', True)
        return self.http_request('GET', endpoint, **kwargs)

    def post(self, endpoint: str, **kwargs):
        return self.http_request('POST', endpoint, **kwargs)

    def put(self, endpoint: str, **kwargs):
        return self.http_request('PUT', endpoint, **kwargs)

    def delete(self, endpoint: str, **kwargs):
        return self.http_request('DELETE', endpoint, **kwargs)


class APIClient(Connection):
    """Abstracts Connection configuration from an application config file.
    Provides context manager for Connection.

    Should be passed a configuration file containing the following keys (evaluation of
    naming options for each key occurs in the order listed):
        * `host` or `url` to be used by the Connection object as a base url (overwritten if `url` kwarg set)
        * `auth` dictionary to be used by the HTTPAuthenticator (or a Request's lib AuthBase object: must be
           callable class which updates a prepared request when called.)
        * `login` dictionary to be used by HTTPLogin
        * `logout` dictionary to be used by the logout handler
        * `proxies` or `proxy` to set session proxies (string or Requests compliant dictionary)
          (overwritten if `proxies` kwarg set)
        * (optional) any other attributes to be passed to requests.Session, i.e., 'headers', 'cookies',
          'auth', 'proxies', 'hooks', 'params', 'verify', 'cert', 'prefetch', 'adapters', 'stream',
          'trust_env', 'max_redirects'. Any passed attribute will be overwritten if also set passed via
          kwargs during initialization.

    Usage Examples:
    .. code-block:: python
        >>>class MyClient(APIClient):
        >>>    def __init__(self, configuration):
        >>>        super(MyClient, self).__init__(configuration, verify=False, log=True)
        >>>
        >>>    def response_handler(self, r, **kwargs):
        >>>        return r.raise_for_status() or r.json()
        >>>
        >>>    def get_important_data(self):
        >>>        return self.get('resource/data', params={'tag': 'important'})


        >>>config = {"host": "https://myhost/api/v1.0/",
        >>>          "auth": {
        >>>             "headers": {
        >>>                 "Authorization": "Token 111A222B333C444D"
        >>>             }
        >>>          }
        >>>}

        >>>with MyClient(config) as mine:
        >>>    important_json = mine.get_important_data()

    :param configuration: dict, client session configuration (necessary keys and evaluation method defined above)
    :param log: (optional) bool, logs all requests upon responses if True
    :param retry_on_status: (optional) HTTP Status Codes, status codes on which to force a retry, type int or list
                            parameters get appended to the DEFAULT_RETRY_CODES, type set parameters overwrite the
                            default, set as False to disable retries .
    :param kwargs: any other attribute(s) to be set on requests.Session plus optional `retry` or `retries` dictionary to
                   be passed to the retry_handler. Optionally use kwargs to overwrite configuration file values, such as
                   url, auth, proxies
    """

    def __init__(self, configuration: dict = None, log=True, retry_on_status=None, **kwargs):

        configuration = configuration or {}

        kwargs.setdefault('url', configuration.get('host', configuration.get('url', None)))
        kwargs.setdefault('auth', self.auth_handler(**configuration) if not kwargs.__contains__('auth') else None)
        kwargs.setdefault('proxies', self.proxy_handler(**configuration) if not kwargs.__contains__('proxies') else None)
        kwargs.setdefault('logout', self.logout_handler(**configuration) if not kwargs.__contains__('logout') else None)
        kwargs.setdefault('timeout', int_tuple_eval(configuration.get('timeout', DEFAULT_TIMEOUT)))

        if retry_on_status is not False:
            try:
                status_codes = retry_on_status if isinstance(retry_on_status, (set, type(None))) \
                    else set(DEFAULT_RETRY_CODES + retry_on_status)

            except TypeError:
                status_codes = set(DEFAULT_RETRY_CODES + [retry_on_status])

            retries = kwargs.pop('retry', kwargs.pop('retries', configuration.get('retry', configuration.get('retries', {}))))
            retries.setdefault('status_forcelist', status_codes or DEFAULT_RETRY_CODES)
            kwargs.setdefault('adapter', self.retry_handler(**retries) if not kwargs.__contains__('adapter') else None)

        if log:
            _hooks = append_hooks(kwargs.pop('hooks', {}), dict(response=print_request))
            kwargs.update(dict(hooks=_hooks))

        [kwargs.setdefault(key, value) for key, value in configuration.items() if key in requests.Session.__attrs__]
        super(APIClient, self).__init__(**kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def auth_handler(self, **kwargs):
        """Configures client authentication based on the configuration file."""
        login_configuration = deepcopy(kwargs.pop('login', None))
        auth_configuration = self.credential_handler(deepcopy(kwargs.pop('auth', None)))

        try:
            login = HTTPLogin(**login_configuration, auth=auth_configuration)
            auth_configuration = self.login_handler(**login())
            login.close()

        except TypeError:
            pass

        try:
            return HTTPAuthenticator(**auth_configuration)

        except TypeError:
            # app.logger.info("Auth not set in configuration file, session created without "
            #                 "authentication. Set `auth=None` to disable alert.")

            return None

    def credential_handler(self, auth: dict) -> dict:
        """
        Overwrite method in subclass to process authentication credentials prior to their
        addition to the Connection. Primarily used for implementing specific encoding or
        formatting before passing the credentials.
        """

        return auth

    def login_handler(self, r, **kwargs) -> dict:
        """
        Functions identically to a response handler for the session: overwrite method
        in subclass to process the response object from the login event. The returned
        object is processed by the auth handler and therefore must match the input
        signature for an HTTPAuthenticator object. If you need to reuse auth attributes
        from the login event within the session, be sure to extract these from the
        response (r.request for the prepared request) and include within your returned
        dictionary.
        """

        return r.json()

    def logout_handler(self, **kwargs):
        """Configures the logout event for use within context manager or class to client close method"""
        try:
            logout_configuration = deepcopy(kwargs.pop('logout', None))
            method = logout_configuration.pop('method', logout_configuration.pop('http-method', logout_configuration.pop('http_method', None)))
            endpoint = logout_configuration.pop('endpoint', logout_configuration.pop('url', None))

            if not method:
                raise KeyError("HTTP method must be defined for logout event: add 'method' or 'http-method' key")

            if not endpoint:
                endpoint = ''
                app.logger.info("Logout endpoint undefined: logout request will be sent to the base url, "
                                "add 'endpoint' or 'url' key in logout configuration (use empty string to "
                                "disable this alert).")

            def execute():
                return self.http_request(method, endpoint, **logout_configuration)

            return execute

        except AttributeError:
            def not_implemented(): pass
            return not_implemented

    @staticmethod
    def proxy_handler(**kwargs):
        """Configures proxies for use by the session, based on the configuration file (or user input: dev-only)."""
        proxy = deepcopy(kwargs.pop('proxies', kwargs.pop('proxy', {})))

        # Temp code for running tasks in dev
        try:
            proxy.format()

        except KeyError:
            app.logger.warn("User input functionality for proxy authentication not supported within production.")
            proxy = proxy.format(uid=urlquote(input("Proxy authentication, username: ")),
                                 pwd=urlquote(getpass("Proxy authentication, password: ")))

        except AttributeError:
            pass

        proxies = {'http': proxy, 'https': proxy} if isinstance(proxy, str) else proxy

        return proxies

    @staticmethod
    def retry_handler(**kwargs):
        """Implements HTTP Adapter with a urllib Retry class to auto-retry failed requests.

        Note: currently these settings are overwritten if you pass an `adapter` key word
        argument to the client, this behavior may be changed in the future if significant
        use cases exist to implement custom adapters while maintaining 'default' retry logic.
        """

        kwargs.setdefault('total', DEFAULT_MAX_RETRIES)
        kwargs.setdefault('connect', DEFAULT_RETRIES)
        kwargs.setdefault('read', DEFAULT_RETRIES)
        kwargs.setdefault('status', DEFAULT_RETRIES)
        kwargs.setdefault('status_forcelist', DEFAULT_RETRY_CODES)
        kwargs.setdefault('backoff_factor', DEFAULT_BACKOFF_FACTOR)
        retries = Retry(**kwargs)

        return HTTPAdapter(max_retries=retries)


class HTTPAuthenticator(object):
    """Attaches HTTP Authentication to the given Request object."""

    def __init__(self, **kwargs):
        self.headers = kwargs.pop('headers', kwargs.pop('header', None))
        self.params = kwargs.pop('params', kwargs.pop('parameters', None))
        self.cookies = kwargs.pop('cookies', kwargs.pop('cookie', None))
        self.data = kwargs.pop('data', kwargs.pop('body', None))
        [delattr(self, attr) for attr in list(self.__dict__) if not getattr(self, attr)]

    def request_updater(function):

        @wraps(function)
        def wrapped(inst, r):
            try:
                return function(inst, r)

            except AttributeError:
                return r

        return wrapped

    @request_updater
    def register_headers(self, r):
        for key, value in self.headers.items():
            r.headers[key] = value

        return r

    @request_updater
    def register_params(self, r):
        r.prepare_url(r.url, self.params)
        return r

    @request_updater
    def register_cookies(self, r):
        _cookies = merge_cookies(r._cookies, self.cookies)
        del r.headers['Cookie']
        r.prepare_cookies(_cookies)
        return r

    @request_updater
    def register_data(self, r):
        r.prepare_body(self.data, files=None, json=None)
        return r

    def __call__(self, r):
        r = self.register_headers(r)
        r = self.register_params(r)
        r = self.register_cookies(r)
        r = self.register_data(r)
        return r


class HTTPLogin(APIClient):
    """Callable API Client for managing login-based authentication within the
    API Client auth handler. Object not intended for external consumption.
    """

    def __init__(self, **kwargs):
        self.method = kwargs.pop('method', kwargs.pop('http-method', kwargs.pop('http_method', None)))
        self.url = kwargs.pop('url', kwargs.pop('host', None))
        self.endpoint = kwargs.pop('endpoint', '')

        if not self.method:
            raise KeyError("HTTP method must be defined for login event: add 'method' or 'http-method' key")

        if not self.url:
            raise URLRequired("Login URL missing: add 'host' or 'url' key in login configuration.")

        elif not urlparse(self.url).scheme:
            raise MissingSchema("Login URL must be absolute path, providing a connection scheme e.g., https://hostname")

        super(HTTPLogin, self).__init__(configuration=kwargs, url=self.url, log=True)

    def response_handler(self, r, **kwargs):
        return dict(r=r, **kwargs)

    def __call__(self, *args, **kwargs):
        return self.http_request(self.method, self.endpoint)
