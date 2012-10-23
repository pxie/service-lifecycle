require "cfoundry"
require "uuidtools"
require "json"
require "utils/results"
require "vcap/logging"

module Utils
  module Action
    module_function

    include Utils::Results

    logfile     = "testing.log"
    loglevel    = :debug
    config = {:level => loglevel, :file => logfile}
    VCAP::Logging.setup_from_config(config)
    $log = VCAP::Logging.logger(File.basename($0))

    def think(thinktime)
      rand = Random.new
      sleep(rand(20) / 20.0 * thinktime)
    end

    ################### HTTP Action ############################
    def create_datastore(uri, service_name)
      path = URI.encode_www_form("service" => service_name)
      url = "#{uri}/createdatastore?#{path}"
      begin
        puts "create datastore. service: #{service_name}"
        $log.info("create datastore. service: #{service_name}")
        $log.debug("POST URL: #{url}")
        response = RestClient.post(url, "", {})
        $log.debug("response: #{response.code}, body: #{response.body}")
      rescue Exception => e
        $log.error("fail to create datastore. url: #{url}, service: #{service_name}\n#{e.inspect}")
      end
    end

    def load_data(uri, service_name, parameters)
      result = "pass"
      path = URI.encode_www_form({"service"     => service_name,
                                   "crequests"  => parameters["crequests"],
                                    "size"      => parameters["size"],
                                    "loop"      => parameters["loop"],
                                    "thinktime" => parameters["thinktime"]})
      url = "#{uri}/insertdata?#{path}"
      begin
        puts "Load data to datastore. service: #{service_name}, params: #{parameters}"
        $log.info("Load data to datastore. service: #{service_name}, params: #{parameters}")
        $log.debug("PUT URL: #{url}")
        response = RestClient.put(url, "", {})
        $log.debug("response: #{response.code}, body: #{response.body}")
      rescue Exception => e
        $log.error("fail to load data. url: #{url}, params#{parameters}\n#{e.inspect}")
        result = "fail"
      end
      insert_result(get_service_domain(uri), "Load Data", result)
    end

    def validate_data(uri, service_name)
      result = "pass"
      path = URI.encode_www_form({"service"     => service_name})
      url = "#{uri}/validatedata?#{path}"
      begin
        puts "validate data. service: #{service_name}"
        $log.info("validate data. service: #{service_name}")
        $log.debug("GET URL: #{url}")
        response = RestClient.get(url)
        $log.debug("response: #{response.code}, body: #{response.body}")
      rescue Exception => e
        $log.error("fail to validate data. url: #{url}\n#{e.inspect}")
        result = "fail"
      end
      insert_result(get_service_domain(uri), "Validate Data", result)
    end

    def take_snapshot(uri, service, header)
      result = "pass"
      path = URI.encode_www_form({"service" => service})
      url = "#{uri}/snapshot/create?#{path}"
      begin
        puts "create snaphsot. url: #{url}"
        $log.info("create snaphsot. url: #{url}")
        response = RestClient.post(url, "", header)
        $log.debug("response: #{response.code}, body: #{response.body}")
        job = JSON.parse(response.body)
        job = wait_job(uri,header, service,job["job_id"])
        if job.is_a?(Hash) && job["status"] && job["status"] == "completed"
          result = "pass"
        else
          result = "fail"
        end
      rescue Exception => e
        $log.error("fail to create snaphost. url: #{url}\n#{e.inspect}")
        result = "fail"
      end
      insert_result(get_service_domain(uri), "Take Snapshot", result)
      response
    end

    def list_snapshot(uri, service, header, snapshot_id = nil)
      result = "pass"
      path = URI.encode_www_form({"service"     => service,
                                  "snapshotid"  => snapshot_id})
      url = "#{uri}/snapshot/list?#{path}"
      begin
        puts "list snapshot. url: #{url}"
        $log.info("list snapshot. url: #{url}, service: #{service}," +
                      " snapshot_id: #{snapshot_id.inspect}")
        timeout = 5 * 60 * 60 # wait 30 mins
        sleep_time = 1
        while timeout > 0
          sleep(sleep_time)
          timeout -= sleep_time

          response = RestClient.post(url, "", header)
          $log.debug("response: #{response.code}, body: #{response.body}")
          break if response.code == 200 && has_snapshot?(response.body)
        end
      rescue Exception => e
        $log.error("fail to list snapshot. url: #{url}, "+
                       "service: #{service}, snapshot_id: #{snapshot_id.inspect}\n#{e.inspect}")
        result = "fail"
      end
      insert_result(get_service_domain(uri), "Take Snapshot", result)
      result == "pass" ? response.body : {"snapshots" => []}.to_json
    end

    def validate_snapshot(uri, service, header, totalnum)
      result = "pass"
      path = URI.encode_www_form({"service"     => service})
      url = "#{uri}/snapshot/list?#{path}"
      begin
        puts "validate snapshot. url: #{url}"
        $log.info("validate snapshot. url: #{url}, service: #{service}")

        response = RestClient.post(url, "", header)
        $log.debug("response: #{response.code}, body: #{response.body}")
        if response.code == 200
          snapshots = JSON.parse(response.body)
          $log.debug("snapshots length: #{snapshots["snapshots"].length}, totalnum: #{totalnum}")
          if snapshots["snapshots"].length == totalnum
            result = "pass"
          else
            result = "fail"
          end
        end
      rescue Exception => e
        $log.error("fail to list snapshot. url: #{url}, service: #{service}\n#{e.inspect}")
        result = "fail"
      end
      insert_result(get_service_domain(uri), "Validate snapshot", result)
      result == "pass" ? true : false
    end

    def has_snapshot?(json_body)
      snapshots = JSON.parse(json_body)
      !snapshots["snapshots"].empty?
    end

    def delete_snapshot(uri, service, header, snapshot_id)
      result = "pass"
      path = URI.encode_www_form({"service"     => service,
                                  "snapshotid"  => snapshot_id})
      url = "#{uri}/snapshot/delete?#{path}"
      begin
        puts "delete snapshot. url: #{url}"
        $log.info("delete snapshot. url: #{url}, service: #{service}," +
                      " snapshot_id: #{snapshot_id.inspect}")
        response = RestClient.post(url, "", header)
        $log.debug("response: #{response.code}, body: #{response.body}")
        job = JSON.parse(response.body)
        job = wait_job(uri,header, service,job["job_id"])
        if job.is_a?(Hash) && job["result"] && job["result"]["result"] == "ok"
          result = "pass"
        else
          result = "fail"
        end
      rescue Exception => e
        $log.error("fail to delete snapshot. url: #{url}, "+
                       "service: #{service}, snapshot_id: #{snapshot_id.inspect}\n#{e.inspect}")
        result = "fail"
      end
      insert_result(get_service_domain(uri), "Delete Snapshot", result)
    end

    def rollback_snapshot(uri, service, header, snapshot_id)
      result = "pass"
      path = URI.encode_www_form({"service"     => service,
                                  "snapshotid"  => snapshot_id})
      url = "#{uri}/snapshot/rollback?#{path}"
      begin
        puts "rollback snapshot. url: #{url}"
        $log.info("rollback snapshot. url: #{url}, service: #{service}," +
                      " snapshot_id: #{snapshot_id.inspect}")
        response = RestClient.post(url, "", header)
        job = JSON.parse(response.body)
        job = wait_job(uri,header,service,job["job_id"])
        if !(job.is_a?(Hash) && job["result"] && job["result"]["result"] && job["result"]["result"] == "ok")
          result = "fail"
        end

        $log.debug("response: #{response.code}, body: #{response.body}")
      rescue Exception => e
        $log.error("fail to rollback snapshot. url: #{url}, "+
                       "service: #{service}, snapshot_id: #{snapshot_id.inspect}\n#{e.inspect}")
        result = "fail"
      end
      insert_result(get_service_domain(uri), "Rollback Snapshot", result)
    end

    def wait_job(uri, header, service, job_id)
      return {} unless job_id
      timeout = 10 * 60 * 60
      sleep_time = 10
      while timeout > 0
        sleep sleep_time
        timeout -= sleep_time

        path = URI.encode_www_form({"service"     => service,
                                    "jobid"       => job_id})
        url = "#{uri}/snapshot/queryjobstatus?#{path}"
        begin
          $log.debug("query job status. url: #{url}")
          response = RestClient.get(url, header)
          job = JSON.parse(response.body)
          $log.debug("query job status. Job: #{job}")
          return job if job["status"] == "completed" || job["status"] == "failed"
        rescue Exception => e
          $log.error("fail to query job status. url: #{url}\n#{e.inspect}")
        end
      end
    end

    def import_from_data(uri, service, header, snapshot_id)
      result = "pass"
      path = URI.encode_www_form({"service"     => service,
                                  "snapshotid"  => snapshot_id})
      url = "#{uri}/snapshot/createurl?#{path}"
      begin
        puts "create url. url: #{url}"
        $log.info("create url. url: #{url}, service: #{service}," +
                      " snapshot_id: #{snapshot_id.inspect}")
        response = RestClient.post(url, "", header)
        $log.debug("response: #{response.code}, body: #{response.body}")
        job = JSON.parse(response.body)
        job = wait_job(uri,header,service,job["job_id"])
        if !(job.is_a?(Hash) && job["result"] && job["result"]["url"])
          result = "fail"
          insert_result(get_service_domain(uri), "create serialized URL", result)
          return
        end
        serialized_url = job["result"]["url"]


        path = URI.encode_www_form({"service"     => service,
                                    "snapshotid"  => snapshot_id})
        url = "#{uri}/snapshot/importdata?#{path}"
        body = {"url" => serialized_url}.to_json

        puts "import from data. url: #{url}"
        $log.info("import from data. url: #{url}, service: #{service}," +
                      " snapshot_id: #{snapshot_id.inspect}, body: #{body}")
        response = RestClient.post(url, body, header)
        $log.debug("response: #{response.code}, body: #{response.body}")
        job = JSON.parse(response.body)
        job = wait_job(uri,header,service,job["job_id"])
        if !(job.is_a?(Hash) && job["result"] && job["result"]["snapshot_id"])
          result = "fail"
        end
      rescue Exception => e
        $log.error("fail to import from data. url: #{url}, "+
                       "service: #{service}, snapshot_id: #{snapshot_id.inspect}\n#{e.inspect}")
        result = "fail"
      end
      insert_result(get_service_domain(uri), "Import from Data", result)
      result
    end

    def import_from_url(uri, service, header, snapshot_id)
      result = "pass"
      path = URI.encode_www_form({"service"     => service,
                                  "snapshotid"  => snapshot_id})
      url = "#{uri}/snapshot/createurl?#{path}"
      #url = "#{uri}/snapshot/importurl?#{path}"
      begin
        puts "create url. url: #{url}"
        $log.info("create url. url: #{url}, service: #{service}," +
                      " snapshot_id: #{snapshot_id.inspect}")
        response = RestClient.post(url, "", header)
        $log.debug("response: #{response.code}, body: #{response.body}")
        job = JSON.parse(response.body)
        job = wait_job(uri,header,service,job["job_id"])
        if !(job.is_a?(Hash) && job["result"] && job["result"]["url"])
          result = "fail"
          insert_result(get_service_domain(uri), "create serialized URL", result)
          return
        end
        serialized_url = job["result"]["url"]

        path = URI.encode_www_form({"service"     => service})
        url = "#{uri}/snapshot/importurl?#{path}"
        body = {"url" => job["result"]["url"]}.to_json

        puts "import from url. url: #{url}, body: #{body}"
        $log.info("import from url. url: #{url}, service: #{service}, body: #{body}")
        response = RestClient.post(url, body, header)
        $log.debug("response: #{response.code}, body: #{response.body}")
        job = JSON.parse(response.body)
        job = wait_job(uri,header,service,job["job_id"])
        if !(job.is_a?(Hash) && job["result"] && job["result"]["snapshot_id"])
          result = "fail"
        end
      rescue Exception => e
        $log.error("fail to import from url. url: #{url}, "+
                       "service: #{service}, snapshot_id: #{snapshot_id.inspect}\n#{e.inspect}")
        result = "fail"
      end
      insert_result(get_service_domain(uri), "Import from URL", result)
      [result, serialized_url]
    end

    def random_snapshot(json_body)
      snapshots = JSON.parse(json_body)
      rand = Random.new(Time.now.usec)
      list = snapshots["snapshots"]
      index = rand(list.size)
      snapshot_id = list[index]["snapshot_id"]
      $log.debug("random select snapshot. snapshot id: #{snapshot_id}")
      snapshot_id
    end

    ###################  VMC Action ##############################
    def push_app(manifest, client)
      puts "push application, #{manifest}"
      $log.info("push application, #{manifest}")
      app = client.apps.first
      path = File.join(File.dirname(__FILE__), "../../app/worker")
      path = File.absolute_path(path)
      if app
        sync_app(app, path, manifest, client)
      else
        app = create_app(manifest, path, client)
      end

      app
    end

    def login(target, email, password)
      puts "login target: #{target}, email: #{email}, password: #{password}"
      begin
        client = CFoundry::Client.new(target)
        $log.info("login target: #{target}, email: #{email}, password: #{password}")
        token = client.login({:username => email, :password => password})
        $log.debug("client: #{client.inspect}")
        [token, client]
      rescue Exception => e
        $log.error("Fail to login target: #{target}, email: #{email}, password: #{password}\n#{e.inspect}")
      end
    end

    def create_service(instance_name, manifest, client, uuid)
      services = client.services
      services.reject! { |s| s.provider != manifest["provider"] }
      services.reject! { |s| s.version != manifest["version"] }

      if v2?(client)
        services.reject! do |s|
          s.service_plans.none? { |p| p.name == manifest["plan"].upcase }
        end
      end

      service = services.first

      instance = client.service_instance
      instance_name = "#{instance_name}-#{uuid}"
      instance.name = instance_name

      if v2?(client)
        instance.service_plan = service.service_plans.select {|p| p == manifest["plan"]}.first
        instance.space = client.current_space
      else
        instance.type = service.type
        instance.vendor = service.label
        instance.version = service.version
        instance.tier = manifest["plan"]
      end

      puts "create service instance: #{instance_name} (#{service.label} " +
               "#{service.version} #{manifest["plan"]} #{manifest["provider"]})"
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

    def cleanup(client)
      $log.debug("cleanup all applications and services")
      client.service_instances.each { |s| s.delete! }
      client.apps.each { |app| app.delete! }
    end

    def bind_service(instance, app)
      puts "Binding service: #{instance.name} to application: #{app.name}"
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

    def create_app(manifest, path, client)
      uuid = UUIDTools::UUID.random_create.to_s
      app = client.app
      app.name = "#{manifest["name"]}-#{uuid}"
      app.space = client.current_space if client.current_space
      app.total_instances = manifest["instances"] ? manifest["instances"] : 1
      app.production = manifest["plan"] if v2?(client) && manifest["plan"]

      all_frameworks = client.frameworks
      all_runtimes = client.runtimes
      framework = all_frameworks.select {|f| f.name == manifest["framework"]}.first
      runtime = all_runtimes.select {|f| f.name == manifest["runtime"]}.first

      app.framework = framework
      app.runtime = runtime

      target_base = client.target.split(".", 2).last
      url = "#{manifest["name"]}-#{uuid}.#{target_base}"
      app.urls = [url] if url && !v2?(client)

      app.memory = manifest["memory"]

      begin
        $log.debug("create application #{app.name}")
        app.create!
      rescue Exception => e
        $log.error("fail to create application #{app.name}\n#{e.inspect}")
      end
      map(app, url) if url && v2?(client)

      begin
        upload_app(app, path)
      rescue Exception => e
        $log.error("fail to upload application source. application: #{app.name}, file path: #{path}\n#{e.inspect}")
      end

      if manifest["services"]
        manifest["services"].each do |instance_name, details|
          instance = create_service(instance_name, details, client, uuid)
          bind_service(instance, app)
        end
      end

      start(app)
      app
    end

    def sync_app(app, path, manifest, client)
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

      if manifest["plan"] && v2?(client)
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
        if v2?(client)
          host, domain_name = simple.split(".", 2)
          $log.debug("map url. host: #{host}, domain_name: #{domain_name}")

          domain =
              client.current_space.domains(0, :name => domain_name).first
          $log.error("invalid domain name") unless domain

          route = client.routes(0, :host => host).find do |r|
            r.domain == domain
          end

          unless route
            $log.debug("create route.")
            route = client.route
            route.host = host
            route.domain = domain
            route.organization = client.current_organization
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

    def v2?(client)
      client.is_a?(CFoundry::V2::Client)
    end

    def get_service_domain(uri)
      uri.split(".").first
    end

  end
end
