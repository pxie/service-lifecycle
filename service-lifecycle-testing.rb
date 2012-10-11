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
load_config.each do |scenario, details|
  $log.info("start to test scenario #{scenario}")

  ## start from single thread serial first
  include Utils::Action
  index = 1
  login(target, users[index]["email"], users[index]["password"])
  app = push_app(details["application"])

  #require "ruby-debug"; breakpoint

  # preload data
  uri = app.urls.first
  service_name = details["application"]["services"]["tested_inst"]["name"]
  create_datastore(uri, service_name)

  if s = details["preload"]
    load_data(uri, service_name, s["load"])
  end










  #threads = []
  #details["cusers"].times do |index|
  #  threads << Thread.new do
  #    include Utils::Action
  #    # login
  #    login(target, users[index]["email"], users[index]["password"])
  #
  #    # push app
  #    push_app(details["application"])
  #
  #
  #    # provision service
  #
  #    # bind service
  #
  #    # preload data
  #
  #    # take snapshot
  #
  #    # rollback
  #
  #    # unprovision
  #  end
  #end
  #threads.each { |t| t.join }

end
