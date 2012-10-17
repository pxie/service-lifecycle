require 'digest/sha1'

module Worker
  module Helper
    MILLION = 1 * 1024 * 1024

    def provision_data(size)
      raise RuntimeError, "data size cannot be greater than 5 MB" if size > 5

      data = Random.new(Time.now.usec).bytes(size * MILLION)
      # remove specific charactor: \', \"
      data = data.gsub(/['"]/, "0")
      [data, sha1sum(data)]
    end

    def sha1sum(data)
      Digest::SHA1.hexdigest(data)
    end

    def think(thinktime)
      sleep(rand(20) / 20 * thinktime)
    end

    def parse_header
      @token  = env['HTTP_TOKEN']
      @app    = env['HTTP_APP']? env['HTTP_APP'] : "lifeworker"
      @target = env['HTTP_TARGET']? env['HTTP_TARGET'] : "http://api.cf110.dev.las01.vcsops.com"
      @email  = env['HTTP_EMAIL']? env['HTTP_EMAIL'] : "jli@rbcon.com"
      @passwd = env['HTTP_PASSWORD']? env['HTTP_PASSWORD'] : "goodluck"

      @session = Worker::CFSession.new(:email => @email,
                                       :passwd => @passwd,
                                       :target => @target)
      $log.debug("parse header. target: #{@target}, app: #{@app}")
      if @token
        @session.token = @token
        $log.info("token: #{@token}")
      else
        $log.info("app: #{@app}")
        $log.info("target: #{@target}")
        $log.info("email: #{@email}")
        $log.info("passwd: #{@passwd}")
        @token = @session.login
        $log.info("token: #{@token}")
      end
    end

    def get_service_id(srv_name)
      content = ENV['VCAP_SERVICES']
      $log.debug("get service id. content: #{content}, ")
      service_id = parse_service_id(content, srv_name)
      $log.info("service id: #{service_id}")
      service_id
    end

    # method should be REST method, only [:get, :put, :post, :delete] is supported
    def get_response(method, relative_path = "/", data = nil, second_domain = nil)
      unless [:get, :put, :post, :delete].include?(method)
        $log.error("REST method #{method} is not supported")
        raise RuntimeError, "REST method #{method} is not supported"
      end

      path = relative_path.start_with?("/") ? relative_path : "/" + relative_path

      easy              = Curl::Easy.new
      easy.url          = get_url(second_domain) + path
      easy.resolve_mode = :ipv4
      begin
        case method
          when :get
            $log.debug("Get response from URL: #{easy.url}")
            easy.http_get
          when :put
            $log.debug("Put data: #{data} to URL: #{easy.url}")
            easy.http_put(data)
          when :post
            $log.debug("Post data: #{data} to URL: #{easy.url}")
            easy.http_post(data)
          when :delete
            $log.debug("Delete URL: #{easy.url}")
            easy.http_delete
          else nil
        end
        # Time dependency
        # Some app's post is async. Sleep to ensure the operation is done.
        sleep 0.1
        return easy
      rescue Exception => e
        $log.error("Cannot #{method} response from/to #{easy.url}\n#{e.to_s}")
        raise RuntimeError, "Cannot #{method} response from/to #{easy.url}\n#{e.to_s}"
      end
    end

    def get_url(second_domain = nil)
      # URLs synthesized from app names containing '_' are not handled well
      # by the Lift framework.
      # So we used '-' instead of '_'
      # '_' is not a valid character for hostname according to RFC 822,
      # use '-' to replace it.
      second_domain = "-#{second_domain}" if second_domain
      "#{@app}#{second_domain}.#{@target.gsub("http://api.", "")}".gsub("_", "-")
    end

  end
end
