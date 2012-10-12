require "helpers"

module Worker

  MYSQL_TABLE_NAME =  "data_values"

  module Tasks
    include Worker::Helper

    def create_datastore(service_name)
      case service_name
        when "mysql"
          $log.debug("ready to create mysql datastore")
          create_mysql_datastore
        else
          $log.error("Invalid service: #{service_name}")
      end
    end

    def insert_data(service_name, crequests, size, loop, thinktime)
      begin
        $log.info("prepare data")
        queue = Queue.new
        index = 0
        loop.times do
          crequests.times do
            queue << index
            index += 1
          end
        end
        $log.info("queue size: #{queue.size}")

        threads = []
        crequests.times do
          threads << Thread.new do
            client = get_client(service_name)
            until queue.empty?
              queue.pop
              data, sha1sum = provision_data(size)
              $log.debug("generate data. data length: #{data.length}, sha1sum: #{sha1sum}")

              $log.debug("client: #{client.inspect}")
              do_insert_data(service_name, client, data, sha1sum)
              $log.debug("think time")
              think(thinktime)
            end
            $log.debug("close mysql client. client: #{client.inspect}")
            client.close
          end
          sleep(0.1) # ramp up
        end
        $log.debug("join threads.")
        threads.each { |t| t.join }

        client = get_client(service_name)
        count = get_data_count(service_name, client)
        client.close
        count
      rescue Exception => e
        $log.error("*** FATAL UNHANDLED EXCEPTION ***")
        $log.error("e: #{e.inspect}")
        $log.error("at@ #{e.backtrace.join("\n")}")
        raise RuntimeError, "Fail to insert data to mysql instance\n#{e.inspect}"
      end
    end

    private

    ######## mysql  ###########
    def create_mysql_datastore()
      table = MYSQL_TABLE_NAME
      client = get_mysql_client
      $log.debug("client: #{client}")
      result = client.query("SELECT table_name FROM information_schema.tables WHERE table_name = '#{table}'");
      result = client.query("Create table IF NOT EXISTS #{table} " +
                                "( id MEDIUMINT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                                "data LONGTEXT, sha1sum varchar(50)); ") if result.count != 1
      $log.info("create table: #{table}. result: #{result.inspect}, client: #{client.inspect}")
      [table, "table"]
    end

    def do_insert_data(service_name, client, data, sha1sum )
      case service_name
        when "mysql"
          client.query("insert into data_values (data, sha1sum) values('#{data}','#{sha1sum}');")
          $log.debug("client: #{client.inspect}, insert data: #{seed}, sha1sum: #{sha1sum}")
        else
          $log.error("Invalid service: #{service_name}")
      end
    end

    def get_data_count(service_name, client)
      case service_name
        when "mysql"
          result = client.query("select * from #{MYSQL_TABLE_NAME};")
          result.count
        else
          $log.error("Invalid service: #{service_name}")
      end
    end



    ############################################
    ## get client connection

    def get_client(service_name)
      case service_name
        when "mysql"
          get_mysql_client()
        else
          $log.error("Invalid service: #{service_name}")
      end
    end

    def get_mysql_client
      mysql_service = load_service('mysql')
      client = Mysql2::Client.new(:host => mysql_service['hostname'],
                                  :username => mysql_service['user'],
                                  :port => mysql_service['port'],
                                  :password => mysql_service['password'],
                                  :database => mysql_service['name'])
    end

    def load_service(service_name)
      services = JSON.parse(ENV['VCAP_SERVICES'])
      service = nil
      services.each do |k, v|
        v.each do |s|
          if k.split('-')[0].downcase == service_name.downcase
            service = s["credentials"]
          end
        end
      end
      service
    end
  end
end