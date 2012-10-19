require "cfoundry"
require "yaml"

module Utils
  USERS_CONFIG      =   File.join(File.dirname(__FILE__), "../../config/users.yml")

  module Helpers
    module_function

    def create_users()
      @user_config = YAML.load_file(USERS_CONFIG) unless @user_config

      start_index   = @user_config["startfrom"]
      end_index     = @user_config["startfrom"] + @user_config["sum"]

      index         = @user_config["email"].index("@")
      raise RuntimeError, "Invalid email account pattern, #{@user_config["email"]}" unless index
      prefix  = @user_config["email"].slice(0, index)
      postfix = @user_config["email"].slice(index, @user_config["email"].length)

      @user_config["users"] = [] unless @user_config["users"]

      puts "http://#{@user_config["control_domain"]}"
      client = CFoundry::Client.new("http://#{@user_config["control_domain"]}")
      (start_index...end_index).to_a.each do |index|
        email = "#{prefix}#{index}#{postfix}"
        password = @user_config["password"]
        puts "create user: #{email}"
        client.register(email, password)
        @user_config["users"] << {"email" => email, "password" => password}
      end
      File.open(USERS_CONFIG, "w") { |f| f.write YAML.dump(@user_config) }
    end

  end
end
