require "cfoundry"
require "logger"

module Worker
  class CFSession
    attr_reader :log, :namespace, :TARGET, :email, :passwd, :is_admin
    attr_accessor :token

    def initialize(options = {})
      options = {:admin => false,
                 :email => nil,
                 :passwd => nil,
                 :target => nil}.merge(options)
      @is_admin = options[:admin]
      #@email = options[:email] ? options[:email] : get_login_email(@is_admin)
      #@passwd = options[:passwd] ? options[:passwd] : get_login_passwd(@is_admin)
      @email = options[:email]
      @passwd = options[:passwd]
      domain_url = options[:target] ? options[:target] : get_target
      @TARGET = domain_url =~ /^http:\/\/api\./ ? domain_url : "http://api.#{domain_url}"

      @log = get_logger
      @namespace = get_namespace
      #login
      #check_privilege(@is_admin)
    end

    def inspect
      "#<BVT::Harness::CFSession '#@TARGET', '#@email'>"
    end

    def login
      @log.debug("Login in, target: #{@TARGET}, email = #{@email}")
      @client = CFoundry::Client.new(@TARGET)
      begin
        @token = @client.login({:username => @email, :password =>  @passwd})
      rescue Exception => e
        puts e.to_s
        @log.error "Fail to login in, target: #{@TARGET}, user: #{@email}"
        raise "Cannot login target environment:\n" +
              "target = '#{@TARGET}', user: '#{@email}'.\n" +
              "Pleae check your ENV and #{VCAP_BVT_CONFIG_FILE}"
      end
      # TBD - ABS: This is a hack around the 1 sec granularity of our token time stamp
      #sleep(1)
    end

    def logout
      @log.debug "logout, target: #{@TARGET}, email = #{@email}"
      @client = nil
    end

    def info
      @log.debug "get target info, target: #{@TARGET}"
      @client.info
    end

    def register(email, password)
      @log.debug("Register user: #{email}")
      BVT::Harness::User.new(@client.register(email, password), self)
    end

    def system_frameworks
      @log.debug "get system frameworks, target: #{@TARGET}"
      @info ||= @client.info
      @info[:frameworks] || {}
    end

    def system_runtimes
      @log.debug "get system runtimes, target: #{@TARGET}"
      @info ||= @client.info
      runtimes = {}
      @info[:frameworks].each do |_, f|
        f[:runtimes].each do |r|
          runtimes[r[:name]] = r
        end
      end
      runtimes
    end

    def system_services
      @log.debug "get system services, target: #{@TARGET}"
      services = {}
      @client.services.each do |service_info|
        service = service_info[0]
        if services[service.label]
          versions  = services[service.label][:versions] || []
          plans     = services[service.label][:plans] || []
          providers = services[service.label][:providers] || []
        else
          versions  = []
          plans     = []
          providers = []
        end
        versions << service.version.to_s unless versions.index(service.version.to_s)
        service_info[1].each do |plan|
          plans << plan unless plans.index(plan)
        end
        providers << service_info[2] unless providers.index(service_info[2])
        services[service.label] = {:versions => versions, :plans => plans,
          :providers => providers}
      end
      services
    end

    def app(name, prefix = '')
      BVT::Harness::App.new(@client.app("#{prefix}#{@namespace}#{name}"), self)
    end

    def apps
      @client.apps.collect {|app| BVT::Harness::App.new(app, self)}
    end

    def services
      @client.service_instances.collect {|service| BVT::Harness::Service.new(service, self)}
    end

    def service(name, require_namespace=true)
      if require_namespace
        BVT::Harness::Service.new(@client.service_instance("#{@namespace}#{name}"), self)
      else
        BVT::Harness::Service.new(@client.service_instance(name), self)
      end
    end

    def users
      begin
        @log.debug("Get Users for target: #{@client.target}, login email: #{@email}")
        users = @client.users.collect {|user| BVT::Harness::User.new(user, self)}
      rescue Exception => e
        @log.error("Fail to list users for target: #{@client.target}, login email: #{@email}")
        raise RuntimeError, "Fail to list users for target: " +
            "#{@client.target}, login email: #{@email}\n#{e.to_s}"
      end
    end

    def user(email, options={})
      options = {:require_namespace => true}.merge(options)
      email = "#{@namespace}#{email}" if options[:require_namespace]
      BVT::Harness::User.new(@client.user(email), self)
    end

    # It will delete all services and apps belong to login token via client object
    def cleanup!
      services.each { |service| service.delete }
      apps.each { |app| app.delete }
    end

    private

    def get_logger
      #VCAP::Logging.logger(File.basename($0))
      Logger.new(STDOUT)
    end

    # generate random string as prefix for one test example
    BASE36_ENCODE  = 36
    LARGE_INTEGER  = 2**32
    def get_namespace
      "t#{rand(LARGE_INTEGER).to_s(BASE36_ENCODE)}-"
    end

    def get_login_email(expected_admin = false)
      @config ||= BVT::Harness::RakeHelper.get_config
      if ENV['YETI_PARALLEL_USER']
        @config['user']['email']  = ENV['YETI_PARALLEL_USER']
        @config['user']['passwd'] = ENV['YETI_PARALLEL_USER_PASSWD']
      elsif ENV['VCAP_BVT_USER']
        @config['user']['email'] = ENV['VCAP_BVT_USER']
      end

      expected_admin ? @config["admin"]["email"] : @config["user"]["email"]
    end

    def get_login_passwd(expected_admin = false)
      ## since no password save, once Yeti user want to run single case
      ## rake helper will launch prompter for password input
      require "harness/rake_helper"
      @config ||= BVT::Harness::RakeHelper.get_config
      if expected_admin
        @config["admin"]["passwd"] ||= BVT::Harness::RakeHelper.get_admin_user_passwd
      else
        @config["user"]["passwd"] ||= BVT::Harness::RakeHelper.get_user_passwd
      end

      expected_admin ? @config["admin"]["passwd"] : @config["user"]["passwd"]
    end

    def get_target
      @config["target"]
    end

    def check_privilege(expect_admin = false)
      expect_privilege = expect_admin ? "admin user" : "normal user"
      actual_privilege = admin? ? "admin user" : "normal user"

      if actual_privilege == expect_privilege
        @log.info "run bvt as #{expect_privilege}"
      else
        @log.error "user type does not match. Expected User Privilege: #{expect_privilege}" +
                       " Actual User Privilege: #{actual_privilege}"
        raise RuntimeError, "user type does not match.\n" +
            " Expected User Privilege: #{expect_privilege}" +
            " Actual User Privilege: #{actual_privilege}"
      end
    end

    def admin?
      user = @client.user(@email)
      user.admin?
    end
  end

end



