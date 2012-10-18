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

$log = Logger.new('testing.log', 'daily')
Utils::Results.create_db

#load_config.each do |scenario, details|
#  $log.info("start to test scenario #{scenario}")
#
#  ## start from single thread serial first
#  include Utils::Action
#  index = 1
#  token = login(target, users[index]["email"], users[index]["password"])
#  app = push_app(details["application"])
#
#  # preload data
#  uri = app.urls.first
#  service_name = details["application"]["services"]["tested_inst"]["name"]
#  create_datastore(uri, service_name)
#
#  if s = details["preload"]
#    s["loop"].times do
#      load_data(uri, service_name, s["load"])
#    end
#  end
#
#  header = {"token"   => token,
#            "target"  => "http://#{target}",
#            "app"     => app.name}
#
#  if s = details["take_snapshot"]
#    s["loop"].times do
#      take_snapshot(uri, service_name, header)
#      load_data(uri, service_name, s["load"])
#    end
#  end
#
#  if s = details["rollback"]
#    s["loop"].times do
#      snapshots = list_snapshot(uri, service_name, header)
#
#      unless snapshots
#        raise RuntimeError, "no snapshot is available. abort!"
#      end
#      # random select one snapshot
#      snapshot_id = random_snapshot(snapshots)
#      rollback_snapshot(uri, service_name, header, snapshot_id)
#      delete_snapshot(uri, service_name, header, snapshot_id)
#
#      load_data(uri, service_name, s["load"])
#      take_snapshot(uri, service_name, header)
#
#      if s["import_from_url"]
#        snapshots = list_snapshot(uri, service_name, header)
#        snapshot_id = random_snapshot(snapshots)
#
#        import_from_url(uri, service_name, header, snapshot_id)
#        load_data(uri, service_name, s["load"])
#        take_snapshot(uri, service_name, header)
#        delete_snapshot(uri, service_name, header, snapshot_id)
#      end
#
#      if s["import_from_data"]
#        snapshots = list_snapshot(uri, service_name, header)
#        snapshot_id = random_snapshot(snapshots)
#
#        import_from_data(uri, service_name, header, snapshot_id)
#        load_data(uri, service_name, s["load"])
#        take_snapshot(uri, service_name, header)
#        delete_snapshot(uri, service_name, header, snapshot_id)
#      end
#    end
#  end


load_config.each do |scenario, details|
  $log.info("start to test scenario #{scenario}")

  threads = []
  details["cusers"].times do |index|
    threads << Thread.new do
      $log.info("start to test scenario #{scenario}")

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
        end
      end

      header = {"token"   => token,
                "target"  => "http://#{target}",
                "app"     => app.name}

      if s = details["take_snapshot"]
        s["loop"].times do
          take_snapshot(uri, service_name, header)
          load_data(uri, service_name, s["load"])
        end
      end

      if s = details["rollback"]
        s["loop"].times do
          snapshots = list_snapshot(uri, service_name, header)

          unless snapshots
            raise RuntimeError, "no snapshot is available. abort!"
          end
          # random select one snapshot
          snapshot_id = random_snapshot(snapshots)
          rollback_snapshot(uri, service_name, header, snapshot_id)
          delete_snapshot(uri, service_name, header, snapshot_id)

          load_data(uri, service_name, s["load"])
          take_snapshot(uri, service_name, header)

          if s["import_from_url"]
            snapshots = list_snapshot(uri, service_name, header)
            snapshot_id = random_snapshot(snapshots)

            import_from_url(uri, service_name, header, snapshot_id)
            load_data(uri, service_name, s["load"])
            take_snapshot(uri, service_name, header)
            delete_snapshot(uri, service_name, header, snapshot_id)
          end

          if s["import_from_data"]
            snapshots = list_snapshot(uri, service_name, header)
            snapshot_id = random_snapshot(snapshots)

            import_from_data(uri, service_name, header, snapshot_id)
            load_data(uri, service_name, s["load"])
            take_snapshot(uri, service_name, header)
            delete_snapshot(uri, service_name, header, snapshot_id)
          end
        end
      end
    end
    sleep(1)
  end
  threads.each { |t| t.join }
  print_result
end


