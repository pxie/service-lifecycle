require "logger"
require "cfoundry"
require "restclient"
require "uri"

$:.unshift(File.join(File.dirname(__FILE__), "lib"))
require "utils"

LOAD_CONFIG = File.join(File.dirname(__FILE__), "config/load-manifest.yml")
load_config = YAML.load_file(LOAD_CONFIG)
user_config = YAML.load_file(Utils::USERS_CONFIG)

target = user_config["control_domain"]
users = user_config["users"]



#$log = Logger.new('testing.log', 'daily')
Utils::Results.create_db

load_config.each do |scenario, details|
  $log.info("start to test scenario #{scenario}")

  threads = []
  details["cusers"].times do |index|
    threads << Thread.new do

      ## start from single thread serial first
      include Utils::Action
      token, client = login(target, users[index]["email"], users[index]["password"])

      #cleanup first

      cleanup(client)
      app = push_app(details["application"], client)

      # preload data
      uri = app.urls.first
      service_name = details["application"]["services"]["tested_inst"]["name"]
      create_datastore(uri, service_name)

      if s = details["preload"]
        s["loop"].times do
          load_data(uri, service_name, s["load"])
          think(s["thinktime"])
        end
      end

      header = {"token"   => token,
                "target"  => "http://#{target}",
                "app"     => app.name}

      if s = details["take_snapshot"]
        s["loop"].times do |index|
          $log.info("Take snapshot job. index: #{index}")
          take_snapshot(uri, service_name, header)
          load_data(uri, service_name, s["load"])
          think(s["thinktime"])
        end
      end

      if s = details["rollback"]
        if validate_snapshot(uri, service_name, header, details["take_snapshot"]["loop"])
         s["loop"].times do |index|
            $log.info("Rollback snapshot job. index: #{index}")
            snapshots = list_snapshot(uri, service_name, header)

            if has_snapshot?(snapshots)
              # random select one snapshot
              snapshot_id = random_snapshot(snapshots)
              rollback_snapshot(uri, service_name, header, snapshot_id)
              validate_data(uri, service_name)
              delete_snapshot(uri, service_name, header, snapshot_id)

              load_data(uri, service_name, s["load"])
              take_snapshot(uri, service_name, header)
              think(s["thinktime"])

              if s["import_from_url"]
                snapshots = list_snapshot(uri, service_name, header)
                snapshot_id = random_snapshot(snapshots)

                import_from_url(uri, service_name, header, snapshot_id)
                load_data(uri, service_name, s["load"])
                validate_data(uri, service_name)
                take_snapshot(uri, service_name, header)
                delete_snapshot(uri, service_name, header, snapshot_id)
                think(s["thinktime"])
              end

              if s["import_from_data"]
                snapshots = list_snapshot(uri, service_name, header)
                snapshot_id = random_snapshot(snapshots)

                import_from_data(uri, service_name, header, snapshot_id)
                load_data(uri, service_name, s["load"])
                validate_data(uri, service_name)
                take_snapshot(uri, service_name, header)
                delete_snapshot(uri, service_name, header, snapshot_id)
                think(s["thinktime"])
              end
            end
          end
        end
      end
    end
    sleep(2)
  end
  threads.each { |t| t.join }
  puts "prepare results"
  print_result
end


