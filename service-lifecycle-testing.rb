require "logger"
require "cfoundry"

$:.unshift(File.join(File.dirname(__FILE__), "lib"))
require "lib/utils"


LOAD_CONFIG = File.join(File.dirname(__FILE__), "../../config/load-manifest.yml")



$log = Logger.new('service-lifecycle-perf.log', 'daily')
load_config = YAML.load_file(USERS_CONFIG)
user_config = YAML.load_file(Utils::USERS_CONFIG)
target = user_config["control_domain"]
users = user_config["users"]

load_config.each do |scenario, details|
  $log.info("start to test scenario #{scenario}")

  threads = []
  details["cusers"].times do |index|
    threads << Thread.new do
      include Utils::Action
      # login
      login(target, users[index]["email"], users[index]["password"])

      # push app



      # provision service

      # bind service

      # preload data

      # take snapshot

      # rollback

      # unprovision
    end
  end

end
