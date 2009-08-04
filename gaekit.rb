require 'java'
require 'uri'
require 'openssl'

module GAEKit
  VERSION = '0.3.2'

  module Datastore
    KIND = self.name

    module API
      import com.google.appengine.api.datastore.DatastoreServiceFactory
      import com.google.appengine.api.datastore.KeyFactory
      import com.google.appengine.api.datastore.Entity
      import com.google.appengine.api.datastore.Query
      import com.google.appengine.api.datastore.Text

      def service
        @service ||= DatastoreServiceFactory.datastore_service
      end
      module_function :service
    end

    module Utils
      def to_java(object)
        if object.is_a?(String) && object.length >= 500
          API::Text.new(object)
        else
          object
        end
      end
      module_function :to_java

      def to_ruby(object)
        if object.is_a?(Java::JavaUtil::ArrayList)
          object.to_a
        elsif object.is_a?(API::Text)
          object.value
        else
          object
        end
      end
      module_function :to_ruby
    end

    module Methods
      def get(key)
        Utils.to_ruby(entity.get_property(key.to_s))
      end
      alias :[] :get

      def put(key, value)
        entity { |ent| ent.set_property(key.to_s, Utils.to_java(value)) }
        value
      end
      alias :[]= :put

      def has_key?(key)
        entity.has_property(key.to_s)
      end
      alias :include? :has_key?

      def delete(key)
        entity { |ent| ent.remove_property(key.to_s) }
        nil
      end

      def keys
        entity.properties.map { |key, value| key }
      end

      def values
        entity.properties.map { |key, value| Utils.to_ruby(value) }
      end

    private
      def entity
        ent = API.service.get(API::KeyFactory.create_key(KIND, self.name))
      rescue NativeException
        ent = API::Entity.new(KIND, self.name)
      ensure
        if block_given?
          yield ent
          API.service.put(ent)
        end
      end
    end

    def dump
      entities = {}
      API.service.prepare(API::Query.new(KIND)).as_iterator.each do |ent|
        properties = {}
        ent.properties.each do |key, value|
          properties[key] = Utils.to_ruby(value)
        end
        entities[ent.key.name] = properties
      end
      entities
    end
    module_function :dump
  end

  class Store
    extend Datastore::Methods
  end

  module Logging
    import java.util.logging.Logger
    import java.util.logging.Level

    LEVELS = {
      :fatal => Level::SEVERE,
      :error => Level::SEVERE,
      :warn => Level::WARNING,
      :info => Level::INFO,
      :debug => Level::FINE,
    }

    def log(level, message, exception = nil)
      raise "Invalid log level: #{level}" unless LEVELS.has_key?(level)
      if exception
        traces = exception.backtrace.dup
        message += "\n" + traces.shift +
                   ": #{exception.message} (#{exception.class})\n\tfrom " +
                   traces.join("\n\tfrom ")
      end
      @logger.log(LEVELS[level], message)
    end

    def fatal(*args)
      log(:fatal, *args)
    end

    def error(*args)
      log(:error, *args)
    end

    def warn(*args)
      log(:warn, *args)
    end

    def info(*args)
      log(:info, *args)
    end

    def debug(*args)
      log(:debug, *args)
    end

    def initialize(name = nil)
      if name
        @logger = Logger.getLogger(name)
      else
        @logger = Logger.anonymous_logger
      end
    end

    def self.included(target)
      target.extend self
      target.instance_variable_set(:@logger,
              Logger.getLogger(Logger::GLOBAL_LOGGER_NAME))
    end
  end

  class Logger
    include Logging
  end

  module URLFetch
    import java.net.URL
    import com.google.appengine.api.urlfetch.URLFetchServiceFactory
    import com.google.appengine.api.urlfetch.HTTPMethod
    import com.google.appengine.api.urlfetch.HTTPRequest
    import com.google.appengine.api.urlfetch.HTTPHeader
    import com.google.appengine.api.urlfetch.FetchOptions

    def service
      @service ||= URLFetchServiceFactory.getURLFetchService
    end
    module_function :service
  end

  module HTTPHelper
    RESERVED_CHARACTERS = /[^a-zA-Z0-9\-\.\_\~]/

    def escape(value)
      URI.escape(value.to_s, RESERVED_CHARACTERS)
    end

    def query_string(parameters, delimiter = '&', quote = nil)
      parameters.map do |name, value|
        "#{escape(name)}=#{quote}#{escape(value)}#{quote}"
      end.join(delimiter)
    end
  end

  class HTTPRequest
    include HTTPHelper

    attr_reader :url, :method, :data, :header

    def initialize(url, method, data = nil, header = {})
      @original_url = url
      url = URI.parse(url)
      query = url.query
      url.query = nil
      url.fragment = nil
      @url = url.to_s

      @method = method.to_s.upcase

      @payload = data
      @data = {}
      @data.update(parse(query)) if query
      @data.update(parse(data)) if data

      @header = header
    end

    def parse(data)
      return data unless data.is_a?(String)
      parameters = {}
      data.split('&').each do |pair|
        name, value = pair.split('=')
        parameters[URI.unescape(name)] = URI.unescape(value)
      end
      parameters
    end

    def payload
      @payload.is_a?(Hash) ? query_string(@payload) : @payload
    end

    def to_java_request(option)
      url = URLFetch::URL.new(@original_url)
      method = URLFetch::HTTPMethod.value_of(@method)
      request = URLFetch::HTTPRequest.new(url, method, option)
      request.payload = payload.to_java_bytes if @payload
      @header.each do |name, value|
        request.add_header(URLFetch::HTTPHeader.new(name.to_s, value.to_s))
      end
      request
    end
  end

  class HTTPResponse
    attr_reader :code, :body

    def initialize(response)
      @code = response.response_code
      content = response.content
      @body = String.from_java_bytes(content) if content
      @headers = {}
      response.headers.each do |header|
        @headers[header.name] = header.value
      end
    end

    def [](name)
      @headers[name]
    end

    def key?(name)
      @headers.key?(name)
    end

    def each
      @headers.each do |name, value|
        yield(name, value)
      end
    end
  end

  class HTTP
    class << self
      alias :start :new
    end

    attr_accessor :authenticator

    def initialize(authenticator = nil,
                   allow_truncate = true,
                   follow_redirects = true)
      @authenticator = authenticator
      if allow_truncate
        @option = URLFetch::FetchOptions::Builder.allow_truncate
      else
        @option = URLFetch::FetchOptions::Builder.disallow_truncate
      end
      if follow_redirects
        @option.follow_redirects
      else
        @option.do_not_follow_recirects
      end
      yield(self) if block_given?
    end

    def delete(url, header = {})
      request(url, :DELETE, nil, header)
    end

    def get(url, header = {})
      request(url, :GET, nil, header)
    end

    def head(url, header = {})
      request(url, :HEAD, nil, header)
    end

    def post(url, data, header = {})
      request(url, :POST, data, header)
    end

    def put(url, data, header = {})
      request(url, :PUT, data, header)
    end

    private
    def request(url, method, data = nil, header = {})
      request = HTTPRequest.new(url, method, data, header)
      request.header.update(@authenticator.header(request)) if @authenticator
      HTTPResponse.new(URLFetch.service.fetch(request.to_java_request(@option)))
    end
  end

  module HTTPAuth
    class AbstractAuth
      def header(request)
        @header
      end
    end

    class BasicAuth < AbstractAuth
      def initialize(username, password)
        auth_token = [ "#{username}:#{password}" ].pack('m').chomp
        @header = { 'Authorization' => "Basic #{auth_token}" }
      end
    end

    class OAuth < AbstractAuth
      include HTTPHelper

      OAUTH_VERSION = '1.0'

      def initialize(consumer_key, consumer_secret, token, token_secret)
        @consumer_key = consumer_key
        @consumer_secret = consumer_secret
        @token = token
        @token_secret = token_secret

        # This class supports only 'HMAC-SHA1' at present.
        @signature_method = 'HMAC-SHA1'
      end

      def header(request)
        parameters = oauth_parameters
        parameters[:oauth_signature] = signature(request, parameters)
        parameters = query_string(parameters, ', ', '"')
        { 'Authorization' => 'OAuth ' + parameters }
      end

      def oauth_parameters
        {
          :oauth_consumer_key => @consumer_key,
          :oauth_token => @token,
          :oauth_signature_method => @signature_method,
          :oauth_timestamp => timestamp,
          :oauth_nonce => nonce,
          :oauth_version => OAUTH_VERSION,
        }
      end

      def signature(request, parameters)
        case @signature_method
        when 'PLAINTEXT'
          escape(secret)
        when 'HMAC-SHA1'
          base64(digest_hmac_sha1(signature_base_string(request, parameters)))
        else
          raise "Unknown signature method: #{@signature_method}"
        end
      end

      def signature_base_string(request, parameters)
        method = request.method
        url = request.url
        parameters = parameters.merge(request.data)
        parameters = parameters.sort_by { |name, value| name.to_s }
        queries = query_string(parameters)
        "#{escape(method)}&#{escape(url)}&#{escape(queries)}"
      end

      def secret
        escape(@consumer_secret) + '&' + escape(@token_secret)
      end

      def digest_hmac_sha1(value)
        OpenSSL::HMAC.digest(OpenSSL::Digest::SHA1.new, secret, value)
      end

      def base64(value)
        [ value ].pack('m').chomp.gsub(/\n/, '')
      end

      def timestamp
        Time.now.to_i.to_s
      end

      def nonce
        OpenSSL::Digest::Digest.hexdigest('MD5', "#{Time.now.to_f}#{rand}")
      end
    end
  end
end
