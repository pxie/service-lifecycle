$:.unshift(File.join(File.dirname(__FILE__), "lib"))
require "utils"

desc "cleanup apps/services"
task :cleanup do
  include Utils::Action
  user_config = YAML.load_file(Utils::USERS_CONFIG)
  user_config["users"].each do |user|
    _, client = login(user_config["control_domain"], user["email"], user["password"])
    cleanup(client)
  end
end

desc "create usses"
task :createusers do
  Utils::Helpers::create_users
end

desc "run tests"
task :tests do
  sh('ruby service-lifecycle-testing.rb')
end

