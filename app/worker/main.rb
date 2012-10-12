require 'sinatra'
require 'redis'
require 'json'
require 'mongo'
require 'mysql2'
require 'carrot'
require 'aws/s3'
require 'uri'
require 'pg'

require "logger"
require "json"

$:.unshift(File.join(File.dirname(__FILE__), "lib"))
require "helpers"
require "tasks"
include Worker::Helper
include Worker::Tasks

$log = Logger.new(STDOUT)
$log.level = Logger::DEBUG


############################################################################

post '/createdatastore' do
  begin
    service_name = params[:servicename]
    name, type = create_datastore(service_name)
    $log.debug("in createdatastore, name: #{name}, type: #{type}")
    "Create #{type}: #{name} for #{service_name} successfully"
  rescue Exception => e
    $log.error("#{e.inspect}, #{e.trace_var}")
  end

end

put '/insertdata' do
  service_name  = params[:servicename]
  crequests     = params[:crequests].to_i
  size          = params[:data].to_f
  loop          = params[:loop].to_i
  thinktime     = params[:thinktime].to_f

  record_count  = insert_data(service_name, crequests, size, loop, thinktime)
  "Insert data successfully, records count: #{record_count}"
end

post 'takesnapshot' do
  service_name  = params[:servicename]
end

#put '/service/mysql' do
#  begin
#    crequests   = params[:crequests].to_i
#    size        = params[:data].to_f
#    loop        = params[:loop].to_i
#    thinktime   = params[:thinktime].to_f
#
#    $log.info("prepare data")
#    queue = Queue.new
#    loop.times do
#      seed, data = provision_data(size)
#      sha1sum = sha1sum(data)
#      $log.debug("seed: #{seed}, data length: #{data.length}, sha1sum: #{sha1sum}")
#      crequests.times do
#        queue << [seed, sha1sum]
#      end
#    end
#    $log.info("queue size: #{queue.size}")
#
#    threads = []
#    crequests.times do
#      threads << Thread.new do
#        mysql_service = load_service('mysql')
#        client = Mysql2::Client.new(:host => mysql_service['hostname'],
#                                    :username => mysql_service['user'],
#                                    :port => mysql_service['port'],
#                                    :password => mysql_service['password'],
#                                    :database => mysql_service['name'])
#        until queue.empty?
#          seed, sha1sum = queue.pop
#          _, data = provision_data(size, seed)
#          client.query("insert into data_values (data, sha1sum) values('#{data}','#{sha1sum}');")
#          $log.debug("client: #{client.inspect}, insert data: #{seed}, sha1sum: #{sha1sum}")
#          think(thinktime)
#        end
#        $log.debug("close mysql client. client: #{client.inspect}")
#        client.close
#      end
#      sleep(0.1) # ramp up
#    end
#    $log.debug("join threads.")
#    threads.each { |t| t.join }
#
#    client = get_mysql_client
#    result = client.query("select * from #{MYSQL_TABLE_NAME};")
#    result.count.to_json
#  rescue Exception => e
#    $log.error("*** FATAL UNHANDLED EXCEPTION ***")
#    $log.error("e: #{e.inspect}")
#    $log.error("at@ #{e.backtrace.join("\n")}")
#    raise RuntimeError, "Fail to insert data to mysql instance\n#{e.inspect}"
#  end
#end

get '/data' do
  case params[:name]
    when "mysql"
      mysql_service = load_service('mysql')
      client = Mysql2::Client.new(:host => mysql_service['hostname'],
                                  :username => mysql_service['user'],
                                  :port => mysql_service['port'],
                                  :password => mysql_service['password'],
                                  :database => mysql_service['name'])
      result = client.query("select * from data_values")
      $log.info("records counts: #{result.size}")
      result.each do |row|
        $log.info("row: #{row}")
      end
  end
end


#######legacy code##############################################################################
get '/env' do
  ENV['VCAP_SERVICES']
end

get '/rack/env' do
  ENV['RACK_ENV']
end

get '/' do
  'hello from sinatra'
end

get '/crash' do
  Process.kill("KILL", Process.pid)
end

get '/service/redis/:key' do
  redis = load_redis
  redis[params[:key]]
end

post '/service/redis/:key' do
  redis = load_redis
  redis[params[:key]] = request.env["rack.input"].read
end

post '/service/mongo/:key' do
  coll = load_mongo
  value = request.env["rack.input"].read
  if coll.find('_id' => params[:key]).to_a.empty?
    coll.insert( { '_id' => params[:key], 'data_value' => value } )
  else
    coll.update( { '_id' => params[:key] }, { '_id' => params[:key], 'data_value' => value } )
  end
  value
end

get '/service/mongo/:key' do
  coll = load_mongo
  coll.find('_id' => params[:key]).to_a.first['data_value']
end

not_found do
  'This is nowhere to be found.'
end

post '/service/mysql/:key' do
  client = load_mysql
  value = request.env["rack.input"].read
  key = params[:key]
  result = client.query("select * from data_values where id='#{key}'")
  if result.count > 0
    client.query("update data_values set data_value='#{value}' where id='#{key}'")
  else
    client.query("insert into data_values (id, data_value) values('#{key}','#{value}');")
  end
  client.close
  value
end

get '/service/mysql/:key' do
  client = load_mysql
  result = client.query("select data_value from  data_values where id = '#{params[:key]}'")
  value = result.first['data_value']
  client.close
  value
end

put '/service/mysql/table/:table' do
  client = load_mysql
  client.query("create table #{params[:table]} (x int);")
  client.close
  params[:table]
end

delete '/service/mysql/:object/:name' do
  client = load_mysql
  client.query("drop #{params[:object]} #{params[:name]};")
  client.close
  params[:name]
end

put '/service/mysql/function/:function' do
  client = load_mysql
  client.query("create function #{params[:function]}() returns int return 1234;");
  client.close
  params[:function]
end

put '/service/mysql/procedure/:procedure' do
  client = load_mysql
  client.query("create procedure #{params[:procedure]}() begin end;");
  client.close
  params[:procedure]
end

post '/service/postgresql/:key' do
  client = load_postgresql
  value = request.env["rack.input"].read
  result = client.query("select * from data_values where id = '#{params[:key]}'")
  if result.count > 0
    client.query("update data_values set data_value='#{value}' where id = '#{params[:key]}'")
  else
    client.query("insert into data_values (id, data_value) values('#{params[:key]}','#{value}');")
  end
  client.close
  value
end

get '/service/postgresql/:key' do
  client = load_postgresql
  value = client.query("select data_value from  data_values where id = '#{params[:key]}'").first['data_value']
  client.close
  value
end

put '/service/postgresql/table/:table' do
  client = load_postgresql
  client.query("create table #{params[:table]} (x int);")
  client.close
  params[:table]
end

delete '/service/postgresql/:object/:name' do
  client = load_postgresql
  object = params[:object]
  name = params[:name]
  name += "()" if object=="function" # PG 'drop function' docs: "The argument types to the function must be specified"
  client.query("drop #{object} #{name};")
  client.close
  name
end

put '/service/postgresql/function/:function' do
  client = load_postgresql
  client.query("create function #{params[:function]}() returns integer as 'select 1234;' language sql;")
  client.close
  params[:function]
end

put '/service/postgresql/sequence/:sequence' do
  client = load_postgresql
  client.query("create sequence #{params[:sequence]};")
  client.close
  params[:sequence]
end

post '/service/rabbit/:key' do
  value = request.env["rack.input"].read
  client = rabbit_service
  write_to_rabbit(params[:key], value, client)
  value
end

get '/service/rabbit/:key' do
  client = rabbit_service
  read_from_rabbit(params[:key], client)
end

post '/service/rabbitmq/:key' do
  value = request.env["rack.input"].read
  client = rabbit_srs_service
  write_to_rabbit(params[:key], value, client)
  value
end

get '/service/rabbitmq/:key' do
  client = rabbit_srs_service
  read_from_rabbit(params[:key], client)
end

get '/service/vblob/list' do
  load_vblob
  AWS::S3::Service.buckets(:reload).inspect rescue "list failed: #{$!} at #{$@}"
end

post '/service/vblob/:bucket' do
  load_vblob
  AWS::S3::Bucket.create(params[:bucket]) rescue "create bucket: #{params[:bucket]} failed: #{$!} at #{$@}"
end

get '/service/vblob/:bucket' do
  load_vblob
  AWS::S3::Bucket.find(params[:bucket]).inspect rescue "fetch bucket: #{params[:bucket]} failed: #{$!} at #{$@}"
end

delete '/service/vblob/:bucket' do
  load_vblob
  AWS::S3::Bucket.delete(params[:bucket]) rescue "delete bucket: #{params[:bucket]} failed: #{$!} at #{$@}"
end

post '/service/vblob/:bucket/:object' do
  load_vblob
  AWS::S3::S3Object.store(params[:object], request.body, params[:bucket]) rescue "post object:#{params[:object]} in bucket: #{params[:bucket]} failed: #{$!} at #{$@}"
end

get '/service/vblob/:bucket/:object' do
  load_vblob
  AWS::S3::S3Object.value(params[:object], params[:bucket]) rescue "get object:#{params[:object]} in bucket: #{params[:bucket]} failed: #{$!} at #{$@}"
end

delete '/service/vblob/:bucket/:object' do
  load_vblob
  AWS::S3::S3Object.delete(params[:object], params[:bucket]) rescue "delete object:#{params[:object]} in bucket: #{params[:bucket]} failed: #{$!} at #{$@}"
end

def load_redis
  redis_service = load_service('redis')
  Redis.new({:host => redis_service["hostname"], :port => redis_service["port"], :password => redis_service["password"]})
end

def load_mysql
  mysql_service = load_service('mysql')
  client = Mysql2::Client.new(:host => mysql_service['hostname'], :username => mysql_service['user'], :port => mysql_service['port'], :password => mysql_service['password'], :database => mysql_service['name'])
  result = client.query("SELECT table_name FROM information_schema.tables WHERE table_name = 'data_values'");
  client.query("Create table IF NOT EXISTS data_values ( id varchar(20), data_value varchar(20)); ") if result.count != 1
  $log.debug("mysql client: #{client.inspect}")
  client
end

def load_mongo
  mongodb_service = load_service('mongodb')
  conn = Mongo::Connection.new(mongodb_service['hostname'], mongodb_service['port'])
  db = conn[mongodb_service['db']]
  coll = db['data_values'] if db.authenticate(mongodb_service['username'], mongodb_service['password'])
end

def load_postgresql
  postgresql_service = load_service('postgresql')
  client = PGconn.open(postgresql_service['host'], postgresql_service['port'], :dbname => postgresql_service['name'], :user => postgresql_service['username'], :password => postgresql_service['password'])
  client.query("create table data_values (id varchar(20), data_value varchar(20));") if client.query("select * from pg_catalog.pg_class where relname = 'data_values';").num_tuples() < 1
  client
end

def load_vblob
  vblob_service = load_service('blob')
  AWS::S3::Base.establish_connection!(
    :access_key_id      => vblob_service['username'],
    :secret_access_key  => vblob_service['password'],
    :port               => vblob_service['port'],
    :server             => vblob_service['host']
  ) unless vblob_service == nil
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

def rabbit_service
  service = load_service('rabbitmq')
  Carrot.new( :host => service['hostname'], :port => service['port'], :user => service['user'], :pass => service['pass'], :vhost => service['vhost'] )
end

def rabbit_srs_service
  service = load_service('rabbitmq')
  uri = URI.parse(service['url'])
  host = uri.host
  port = uri.port
  user = uri.user
  pass = uri.password
  vhost = uri.path[1..uri.path.length]
  Carrot.new( :host => host, :port => port, :user => user, :pass => pass, :vhost => vhost )
end

def write_to_rabbit(key, value, client)
  q = client.queue(key)
  q.publish(value)
end

def read_from_rabbit(key, client)
  q = client.queue(key)
  msg = q.pop(:ack => true)
  q.ack
  msg
end
