require "cfoundry"

module Utils

  module Action
    module_function

    def push_app(manifest)
      $log.info("push application, #{manifest}")
      app = @client.app_by_name(manifest["name"])
      path = File.join(File.dirname(__FILE__), "../../app/worker")
      if app
        sync_app(app, path, manifest)
      else
        create_app(manifest, path)
      end

    end

    def login(target, email, password)
      begin
        @client = CFoundry::Client.new(target)
        $log.info("login target: #{target}, email: #{email}, password: #{password}")
        @client.login({:username => email, :password => password})
        @client
      rescue Exception => e
        $log.error("Fail to login target: #{target}, email: #{email}, password: #{password}\n#{e.inspect}")
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

      #target_base = @client.target.index(".")

      url =
          if framework.name == "standalone"
            if (given = input[:url, "none"]) != "none"
              given
            end
          else
            input[:url, "#{name}.#{target_base}"]
          end

      app.urls = [url] if url && !v2?

      default_memory = detector.suggested_memory(framework) || 64
      app.memory = megabytes(input[:memory, human_mb(default_memory)])

      app = filter(:create_app, app)

      with_progress("Creating #{c(app.name, :name)}") do
        app.create!
      end

      invoke :map, :app => app, :url => url if url && v2?

      bindings = []

      if input[:create_services] && !force?
        while true
          invoke :create_service, :app => app
          break unless ask "Create another service?", :default => false
        end
      end

      if input[:bind_services] && !force?
        instances = client.service_instances

        while true
          invoke :bind_service, :app => app

          break if (instances - app.services).empty?

          break unless ask("Bind another service?", :default => false)
        end
      end

      app = filter(:push_app, app)

      begin
        upload_app(app, path)
      rescue
        err "Upload failed. Try again with 'vmc push'."
        raise
      end

      invoke :start, :app => app if input[:start]
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

      all_frameworks = client.frameworks
      framework = all_frameworks.select {|f| f.name == manifest["framework"]}.first
      if framework != app.framework
        diff[:framework] = [app.framework.name, framework.name]
        app.framework = framework
      end

      all_runtimes = client.runtimes
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

    def stop(app)
      begin
        $log.info("stop application #{app.name}")
        app.stop! unless app.stopped?
      rescue Exception => e
        $log.error("fail to stop application #{app.name}\n#{e.inspect}")
      end
    end

    def start(app)
      begin
        $log.info("start application #{app.name}")
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