
module Worker
  module Actions
    module_function

    MYSQL_TABLE_NAME =  "data_values"

    def createdatastore(service_name)
      case service_name
        when "mysql"
          create_mysql_datastore(MYSQL_TABLE_NAME)
        else
          $log.error("invalid service: #{service_name}")
      end

    end

    private

    def create_mysql_datastore(table)
      client = get_mysql_client
      $log.debug("create_mysql_datastore. client: #{client.inspect}")
      result = client.query("SELECT table_name FROM information_schema.tables WHERE table_name = '#{table}'");
      result = client.query("Create table IF NOT EXISTS #{table} " +
                                "( id MEDIUMINT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                                "data LONGTEXT, sha1sum varchar(50)); ") if result.count != 1
      $log.info("create table: #{table}. result: #{result.inspect}, client: #{client.inspect}")
      client.close
      {:state => "OK", :result => result.inspect, :client => client.inspect}.to_json
    end

    def get_mysql_client
      mysql_service = load_service('mysql')
      Mysql2::Client.new(:host => mysql_service['hostname'],
                          :username => mysql_service['user'],
                          :port => mysql_service['port'],
                          :password => mysql_service['password'],
                          :database => mysql_service['name'])
    end


  end
end