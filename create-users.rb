$:.unshift(File.join(File.dirname(__FILE__), "lib"))
require "utils"

Utils::Helpers::create_users
