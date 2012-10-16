require "cfoundry"
require "uuidtools"

module Utils
  module Action
    module_function



    ################### HTTP Action ############################
    def create_datastore(uri, service_name)
      path = URI.encode_www_form("service" => service_name)
      url = "#{uri}/createdatastore?#{path}"
      begin
        $log.info("create datastore. service: #{service_name}")
        $log.debug("POST URL: #{url}")
        response = RestClient.post(url, "", {})
        $log.debug("response: #{response.code}, body: #{response.body}")
      rescue Exception => e
        $log.error("fail to create datastore. url: #{url}, service: #{service_name}\n#{e.inspect}")
      end
    end

    def load_data(uri, service_name, parameters)
      path = URI.encode_www_form({"service"     => service_name,
                                   "crequests"  => parameters["crequests"],
                                    "data"      => parameters["data"],
                                    "loop"      => parameters["loop"],
                                    "thinktime" => parameters["thinktime"]})
      url = "#{uri}/insertdata?#{path}"
      begin
        $log.info("Load data to datastore. service: #{service_name}, params: #{parameters}")
        $log.debug("PUT URL: #{url}")
        response = RestClient.put(url, "", {})
        $log.debug("response: #{response.code}, body: #{response.body}")
      rescue Exception => e
        $log.error("fail to load data. url: #{url}, params#{parameters}\n#{e.inspect}")
      end
    end

    ###################  VMC Action ##############################
    def push_app(manifest)
      $log.info("push application, #{manifest}")
      app = @client.app_by_name(manifest["name"])
      path = File.join(File.dirname(__FILE__), "../../app/worker")
      path = File.absolute_path(path)
      if app
        sync_app(app, path, manifest)
      else
        app = create_app(manifest, path)
      end

      app
    end

    def login(target, email, password)
      begin
        @client = CFoundry::Client.new(target)
        $log.info("login target: #{target}, email: #{email}, password: #{password}")
        @client.login({:username => email, :password => password})
        @client
        $log.debug("client: #{@client.inspect}")
      rescue Exception => e
        $log.error("Fail to login target: #{target}, email: #{email}, password: #{password}\n#{e.inspect}")
      end
    end

    def create_service(instance_name, manifest)
      services = @client.services
      services.reject! { |s| s.provider != manifest["provider"] }
      services.reject! { |s| s.version != manifest["version"] }

      if v2?
        services.reject! do |s|
          s.service_plans.none? { |p| p.name == manifest["plan"].upcase }
        end
      end

      service = services.first

      instance = @client.service_instance
      instance.name = instance_name

      if v2?
        instance.service_plan = service.service_plans.select {|p| p == manifest["plan"]}.first
        instance.space = @client.current_space
      else
        instance.type = service.type
        instance.vendor = service.label
        instance.version = service.version
        instance.tier = manifest["plan"]
      end

      begin
        $log.info("create service instance: #{instance_name} (#{service.label} " +
                      "#{service.version} #{manifest["plan"]} #{manifest["provider"]})")
        instance.create!
      rescue Exception => e
        $log.error("fail to create service instance: #{instance_name} (#{service.label} " +
                       "#{service.version} #{manifest["plan"]} #{manifest["provider"]})\n#{e.inspect}")
      end

      instance
    end

    def bind_service(instance, app)
      begin
        $log.info("Binding service: #{instance.name} to application: #{app.name}")
        unless app.binds?(instance)
          app.bind(instance)
        end
      rescue Exception => e
        $log.error("fail to bind service: #{instance.name} to application: #{app.name}\n#{e.inspect}")
      end
    end

    private

    def create_app(manifest, path)
      app = @client.app
      app.name = manifest["name"]
      app.space = @client.current_space if @client.current_space
      app.total_instances = manifest["instances"] ? manifest["instances"] : 1
      app.production = manifest["plan"] if v2? && manifest["plan"]

      all_frameworks = @client.frameworks
      all_runtimes = @client.runtimes
      framework = all_frameworks.select {|f| f.name == manifest["framework"]}.first
      runtime = all_runtimes.select {|f| f.name == manifest["runtime"]}.first

      app.framework = framework
      app.runtime = runtime

      target_base = @client.target.split(".", 2).last
      uuid = UUIDTools::UUID.random_create.to_s
      url = "#{manifest["name"]}-#{uuid}.#{target_base}"
      app.urls = [url] if url && !v2?

      app.memory = manifest["memory"]

      begin
        $log.debug("create application #{app.name}")
        app.create!
      rescue Exception => e
        $log.error("fail to create application #{app.name}\n#{e.inspect}")
      end
      map(app, url) if url && v2?

      begin
        upload_app(app, path)
      rescue Exception => e
        $log.error("fail to upload application source. application: #{app.name}, file path: #{path}\n#{e.inspect}")
      end

      if manifest["services"]
        manifest["services"].each do |instance_name, details|
          instance = create_service(instance_name, details)
          bind_service(instance, app)
        end
      end

      start(app)
      app
    end

    def sync_app(app, path, manifest)
      upload_app(app, path)

      diff = {}
      mem = manifest["memory"]
      if mem != app.memory
        diff[:memory] = [app.memory, mem]
        app.memory = mem
      end

      instances = 1
      if instances != app.total_instances
        diff[:instances] = [app.total_instances, instances]
        app.total_instances = instances
      end

      all_frameworks = @client.frameworks
      framework = all_frameworks.select {|f| f.name == manifest["framework"]}.first
      if framework != app.framework
        diff[:framework] = [app.framework.name, framework.name]
        app.framework = framework
      end

      all_runtimes = @client.runtimes
      runtime = all_runtimes.select {|f| f.name == manifest["runtime"]}.first
      if runtime != app.runtime
        diff[:runtime] = [app.runtime.name, runtime.name]
        app.runtime = runtime
      end

      if manifest["command"]
        command = manifest["command"]

        if command != app.command
          diff[:command] = [app.command, command]
          app.command = command
        end
      end

      if manifest["plan"] && v2?
        production = manifest["plan"]

        if production != app.production
          diff[:production] = [bool(app.production), bool(production)]
          app.production = production
        end
      end

      unless diff.empty?
        $log.debug("difference need to update: #{diff}")
        begin
          $log.debug("update application #{app.name}")
          app.update!
        rescue Exception => e
          $log.error("fail to update application #{app.name}\n#{e.inspect}")
        end
      end

      restart(app)
    end

    def map(app, url)
      simple = url.sub(/^https?:\/\/(.*)\/?/i, '\1')

      begin
        $log.info("bind route: #{url} to application: #{app.name}")
        if v2?
          host, domain_name = simple.split(".", 2)
          $log.debug("map url. host: #{host}, domain_name: #{domain_name}")

          domain =
              @client.current_space.domains(0, :name => domain_name).first
          $log.error("invalid domain name") unless domain

          route = @client.routes(0, :host => host).find do |r|
            r.domain == domain
          end

          unless route
            $log.debug("create route.")
            route = @client.route
            route.host = host
            route.domain = domain
            route.organization = @client.current_organization
            route.create!
          end
          app.add_route(route)
        else
          app.urls << simple
          app.update!
        end
      rescue Exception => e
        $log.error("fail to bind route: #{url} to application: #{app.name}\n#{e.inspect}")
      end
    end

    def stop(app)
      begin
        $log.info("stop application: #{app.name}")
        app.stop! unless app.stopped?
      rescue Exception => e
        $log.error("fail to stop application #{app.name}\n#{e.inspect}")
      end
    end

    def start(app)
      begin
        $log.info("start application: #{app.name}")
        app.start! unless app.started?
      rescue
        $log.error("fail to start application #{app.name}")
      end

      check_application(app)
    end

    def restart(app)
      stop(app)
      start(app)
    end

    APP_CHECK_LIMIT = 60
    def check_application(app)
      seconds = 0
      until app.healthy?
        sleep 1
        seconds += 1
        if seconds == APP_CHECK_LIMIT
          $log.error("application #{app.name} cannot be started in #{APP_CHECK_LIMIT} seconds")
        end
      end
    end

    def upload_app(app, path)
      begin
        $log.debug("upload application source, name: #{app.name}, path: #{path}")
        app.upload(path)
      rescue Exception => e
        $log.error("fail to upload application source, name: #{app.name}, path: #{path}\n#{e.inspect}")
      end
    end

    def v2?
      @client.is_a?(CFoundry::V2::Client)
    end

  end
end
