require "helpers"

module Worker
  module Actions
    module_function

    include Worker::Helper

    MYSQL_TABLE_NAME      = "data_values"
    MYSQL_MAX_BIN_LENGTH  = 5 * 1024 * 1024

    REDIS_TABLE_NAME      = "data_values"
    REDIS_MAX_BIN_LENGTH  = 5 * 1024 * 1024

    POSTGRESQL_TABLE_NAME      = "data_values"
    POSTGRESQL_MAX_BIN_LENGTH  = 5 * 1024 * 1024

    MONGODB_TABLE_NAME      = "data_values"
    MONGODB_MAX_BIN_LENGTH  = 5 * 1024 * 1024

    def createdatastore(service_name)
      eval("create_#{service_name}_datastore")
    end

    def validatedata(service_name)
      eval("validate_#{service_name}_datastore")
    end

    def cleardata(service_name)
      eval("clear_#{service_name}_datastore")
    end

    def insertdata(service_name, crequests, size, loop, thinktime)
      $log.info("prepare data")
      index = 0
      queue = Queue.new
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
            $log.debug("provision data, size: #{size}, sha1sum: #{sha1sum}, thread: #{Thread.current.inspect}")
            #$log.debug("provision data, data: #{data}, size: #{size}, sha1sum: #{sha1sum}, thread: #{Thread.current.inspect}")
            do_insert_data(service_name, client, data, sha1sum)
            think(thinktime)
          end
          $log.debug("close client. client: #{client.inspect}")
          close_connection(service_name, client)
        end
        sleep(1) # ramp up
      end
      $log.debug("join threads.")
      threads.each { |t| t.join }

      count = get_total_counts(service_name)
      {:state => "OK", :count => count}.to_json
    end


    ########### common function ###################

    def get_client(service_name)
      client = nil
      eval("client = get_#{service_name}_client")
      $log.debug("get_client. #{service_name} client: #{client.inspect}")
      client
    end

    def do_insert_data(service_name, client, data, sha1sum)
      eval("insert_data_to_#{service_name}(client, data, sha1sum)")
    end

    def get_total_counts(service_name)
      client = get_client(service_name)
      counts = do_counts(service_name, client)
      close_connection(service_name, client)
      counts
    end

    def do_counts(service_name, client)
      eval("count_#{service_name}_records(client)")
    end

    def close_connection(service_name, client)
      case service_name
        when "mysql"
          client.close
        when "redis"
          client.quit
        when "postgresql"
          client.close
        when "mongodb"
          $log.info("close: #{service_name}")
        else
          $log.error("invalid service: #{service_name}")
      end
    end







    ########## mysql functions ##########
    def create_mysql_datastore
      client = get_mysql_client
      $log.debug("create_mysql_datastore. client: #{client.inspect}")
      result = client.query("SELECT table_name FROM information_schema.tables WHERE table_name = '#{MYSQL_TABLE_NAME}'");
      result = client.query("Create table IF NOT EXISTS #{MYSQL_TABLE_NAME} " +
                                "( id MEDIUMINT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                                "data VARBINARY(#{MYSQL_MAX_BIN_LENGTH}), sha1sum varchar(50)); ") if result.count != 1
      $log.info("create table: #{MYSQL_TABLE_NAME}. result: #{result.inspect}, client: #{client.inspect}")
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

    def insert_data_to_mysql(client, data, sha1sum)
      $log.info("insert data to mysql. client: #{client.inspect}, data size: #{data.size}, sha1sum: #{sha1sum}")
      client.query("insert into #{MYSQL_TABLE_NAME} (data, sha1sum) values ('#{data}', '#{sha1sum}');")
    end

    def count_mysql_records(client)
      result = client.query("select * from #{MYSQL_TABLE_NAME};")
      $log.debug("count_mysql_records. result: #{result.inspect}, counts: #{result.count}")
      result.count
    end

    def validate_mysql_datastore
      client = get_mysql_client
      $log.info("validate mysql datastore. client: #{client.inspect}")
      result = client.query("select * from #{MYSQL_TABLE_NAME};")
      count = result.count
      if count > 0
        rand = Random.new
        8.times do
          index = rand(count)
          $log.debug("validate mysql datastore. SQL: select * from #{MYSQL_TABLE_NAME} where id = #{index}")
          results = client.query("select * from #{MYSQL_TABLE_NAME} where id = #{index};")
          results.each do |r|

            if sha1sum(r["data"]) != r["sha1sum"]
              #$log.debug("r: #{r.inspect}, actual sha1sum: #{sha1sum(r["data"])}, data encoding: #{r["data"].encoding}")
              raise RuntimeError, "index: #{index}, expected sha1sum: #{r["sha1sum"]}"
            end
          end
        end
      end
      client.close
      "OK"
    end

    def clear_mysql_datastore
    end

    ########## redis functions ##########
    def create_redis_datastore
    end

    def get_redis_client
      redis_service = load_service('redis')
      Redis.new(:host => redis_service['host'],
                :port => redis_service['port'],
                :user => redis_service['username'],
                :password => redis_service['password'])
    end

    def insert_data_to_redis(client, data, sha1sum)
      key = sha1sum
      $log.info("insert data to redis. client: #{client.inspect}, key: #{sha1sum}, data size: #{data.size}, sha1sum: #{sha1sum}")
      client.set(key, data)
    end

    def count_redis_records(client)
      keys = client.keys("*")
      $log.debug("count_redis_records. keys: #{keys.inspect}, counts: #{keys.count}")
      keys.count
    end

    def validate_redis_datastore
      client = get_redis_client
      $log.info("validate redis datastore. client: #{client.inspect}")
      keys = client.keys("*")

      count = keys.count
      if count > 0
        rand = Random.new
        8.times do
          index = rand(count)
          key = keys[index]
          $log.debug("validate redis datastore. redis: get key #{key}")
          data = client.get(key)
          #$log.info("result: #{result}")
          $log.info("sha1sum is: #{key}")
          sha1sum_res = sha1sum(data)
          if sha1sum_res != key
            raise RuntimeError, "key: #{key}, expected sha1sum: #{key}, but now: #{sha1sum_res}"
          end
        end
      end
      client.quit
      "OK"
    end

    def clear_redis_datastore
      client = get_redis_client
      $log.info("Clear redis datastore. client: #{client.inspect}")
      keys = client.keys("*")

      count = keys.count
      for index in 0..count do
        key = keys[index]
        client.del(key)
      end
      client.quit
      "OK"
     end

    ########## postgresql functions ##########
    def create_postgresql_datastore
      client = get_postgresql_client
      $log.debug("create_postgresql_datastore. client: #{client.inspect}")
      result = client.query("create table #{POSTGRESQL_TABLE_NAME} (id varchar(50), data text);") if client.query("select * from pg_catalog.pg_class where relname = '#{POSTGRESQL_TABLE_NAME}';").num_tuples() < 1
      $log.info("create table: #{POSTGRESQL_TABLE_NAME}. result: #{result.inspect}, client: #{client.inspect}")
      client.close
      {:state => "OK", :result => result.inspect, :client => client.inspect}.to_json
    end

    def get_postgresql_client
      postgresql_service = load_service('postgresql')
      PGconn.open(postgresql_service['host'],
                  postgresql_service['port'],
                  :dbname => postgresql_service['name'],
                  :user => postgresql_service['username'],
                  :password => postgresql_service['password'])
    end

    def insert_data_to_postgresql(client, data, sha1sum)
      $log.info("insert data to postgresql. client: #{client.inspect}, data size: #{data.size}, sha1sum: #{sha1sum}")
      client.query("insert into #{POSTGRESQL_TABLE_NAME} (id, data) values('#{sha1sum}','#{data}');")
    end

    def count_postgresql_records(client)
      result = client.query("select * from #{POSTGRESQL_TABLE_NAME};")
      $log.debug("count_postgresql_records. result: #{result.inspect}, counts: #{result.count}")
      result.count
    end

    def validate_postgresql_datastore
      client = get_postgresql_client
      $log.info("validate postgresql datastore. client: #{client.inspect}")
      rand = Random.new
      8.times do
        $log.debug("validate postgresql datastore. SQL: select * from #{POSTGRESQL_TABLE_NAME} order by random() limit 1;")
        results = client.query("select * from #{POSTGRESQL_TABLE_NAME} order by random() limit 1;")
        results.each do |r|
          id = r["id"]
          $log.info("validate postgresql datastore. id: #{id};")
          sha1sum_res = sha1sum(r["data"])
          if sha1sum_res != id
            raise RuntimeError, "id: #{id}, expected sha1sum: #{id}, but now: #{sha1sum_res}"
          end
        end
      end
      client.close
      "OK"
    end

    def clear_postgresql_datastore
      client = get_postgresql_client
      $log.info("Clear postgresql datastore. client: #{client.inspect}")
      client.query("delete from #{POSTGRESQL_TABLE_NAME};")
      client.query("drop table #{POSTGRESQL_TABLE_NAME};")
    end

    ########## mongodb functions ##########
    def create_mongodb_datastore
    end

    def get_mongodb_client
      mongodb_service = load_service('mongodb')
      conn = Mongo::Connection.new(mongodb_service['hostname'], mongodb_service['port'])
      db = conn[mongodb_service['db']]
      coll = db[MONGODB_TABLE_NAME] if db.authenticate(mongodb_service['username'], mongodb_service['password'])
    end

    def insert_data_to_mongodb(client, data, sha1sum)
      $log.info("insert data to mongodb. client: #{client.inspect}, data size: #{data.size}, sha1sum: #{sha1sum}")
      client.insert( { '_id' => sha1sum, 'data' => data } )
    end

    def count_mongodb_records(client)
      result = client.find()
      $log.debug("count_mongodb_records. result: #{result.inspect}, counts: #{result.count}")
      result.count
    end

    def validate_mongodb_datastore
      client = get_mongodb_client
      $log.info("validate mongodb datastore. client: #{client.inspect}")
      results = client.find().to_a
      count = results.count
      8.times do
        random = (rand * 10000 % count).to_i
        r = results[random]
        id = r["_id"]
        $log.info("validate postgresql datastore. id: #{id};")
        sha1sum_res = sha1sum(r["data"])
        if sha1sum_res != id
          raise RuntimeError, "id: #{id}, expected sha1sum: #{id}, but now: #{sha1sum_res}"
        end
      end
      "OK"
    end

    def clear_mongodb_datastore
      client = get_mongodb_client
      client.remove({})
    end
  end
end
