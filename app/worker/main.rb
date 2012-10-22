require 'sinatra'
require 'redis'
require 'json'
require 'mongo'
require 'mysql2'
require 'carrot'
require 'uri'
require 'pg'
require 'curb'

require "logger"
require "json"

$:.unshift(File.join(File.dirname(__FILE__), "lib"))
require "actions"
require "helpers"
require "service_lifecycle_helper"

include Worker::Actions
include Worker::Helper
include Worker::ServiceLifecycleHelper

require "cfsession"

$log = Logger.new(STDOUT)
$log.level = Logger::DEBUG

post '/createdatastore' do
  service_name = params[:service]
  createdatastore(service_name)
end

put '/insertdata' do
  service_name  = params[:service]
  crequests     = params[:crequests].to_i
  size          = params[:size].to_f
  loop          = params[:loop].to_i
  thinktime     = params[:thinktime].to_f
  $log.debug("/insertdata. service: #{service_name}, crequests: #{crequests}," +
                 " size: #{size}, loop: #{loop}, thinktime: #{thinktime}")
  insertdata(service_name, crequests, size, loop, thinktime)
end

get '/validatedata' do
  service_name = params[:service]
  validatedata(service_name)
end

get '/cleardata' do
  service_name = params[:service]
  cleardata(service_name)
end

#create snapshot
post '/snapshot/create' do
  begin
    service     = params[:service]
    $log.info("service name: #{service}")

    parse_header
    service_id  = get_service_id(service)
    resp = create_snapshot(service_id)

    resp
  rescue Exception => e
    $log.error("*** FATAL UNHANDLED EXCEPTION ***")
    $log.error("e: #{e.inspect}")
    $log.error("at@ #{e.backtrace.join("\n")}")
    raise e
  end
end

# get job status
get '/snapshot/queryjobstatus' do
  begin
    service     = params[:service]
    job_id      = params[:jobid]
    $log.info("query job status. service name: #{service}")

    parse_header
    service_id  = get_service_id(service)
    job = get_job(service_id, job_id)
    job.to_json
  rescue Exception => e
    $log.error("*** FATAL UNHANDLED EXCEPTION ***")
    $log.error("e: #{e.inspect}")
    $log.error("at@ #{e.backtrace.join("\n")}")
    raise e
  end
end

#list snapshots
post '/snapshot/list' do
  begin
    service     = params[:service]
    snapshot_id = params[:snapshotid]
    $log.info("service name: #{service}")

    parse_header
    service_id  = get_service_id(service)

    if snapshot_id==nil
      resp = get_snapshots(service_id)
    else
      resp = get_snapshot(service_id, snapshot_id)
    end
    resp
  rescue Exception => e
    $log.error("*** FATAL UNHANDLED EXCEPTION ***")
    $log.error("e: #{e.inspect}")
    $log.error("at@ #{e.backtrace.join("\n")}")
    raise e
  end
end

#rollbak snapshot
post '/snapshot/rollback' do
  begin
    service     = params[:service]
    snapshot_id = params[:snapshotid]

    parse_header
    service_id  = get_service_id(service)

    resp = rollback_snapshot(service_id, snapshot_id)
    resp
  rescue Exception => e
    $log.error("*** FATAL UNHANDLED EXCEPTION ***")
    $log.error("e: #{e.inspect}")
    $log.error("at@ #{e.backtrace.join("\n")}")
    raise e
  end
end

#delete snapshot
post '/snapshot/delete' do
  begin
    service     = params[:service]
    snapshot_id = params[:snapshotid]

    parse_header
    service_id  = get_service_id(service)

    resp = delete_snapshot(service_id, snapshot_id)
    resp.to_json
  rescue Exception => e
    $log.error("*** FATAL UNHANDLED EXCEPTION ***")
    $log.error("e: #{e.inspect}")
    $log.error("at@ #{e.backtrace.join("\n")}")
    raise e
  end
end

post '/snapshot/createurl' do
  service     = params[:service]
  snapshot_id = params[:snapshotid]

  parse_header
  service_id  = get_service_id(service)

  create_serialized_url(service_id, snapshot_id)

end
#import service from url
post '/snapshot/importurl' do
  begin
    service     = params[:service]
    snapshot_id = params[:snapshotid]

    parse_header
    request.body.rewind
    body = JSON.parse(request.body.read)
    serialized_url = body["url"]
    service_id  = get_service_id(service)

    #serialized_url = create_serialized_url(service_id, snapshot_id)
    import_service_from_url(service_id, serialized_url)

  rescue Exception => e
    $log.error("*** FATAL UNHANDLED EXCEPTION ***")
    $log.error("e: #{e.inspect}")
    $log.error("at@ #{e.backtrace.join("\n")}")
    raise e
  end
end

#import service from data
post '/snapshot/importdata' do
  begin
    service     = params[:service]
    snapshot_id = params[:snapshotid]

    parse_header
    service_id  = get_service_id(service)

    request.body.rewind
    body = JSON.parse(request.body.read)
    serialized_url = body["url"]

    #serialized_url = create_serialized_url(service_id, snapshot_id)
    $log.debug("import data. serialized_url: #{serialized_url}")
    serialized_data = download_data(serialized_url)
    import_service_from_data(service_id, serialized_data)
  rescue Exception => e
    $log.error("*** FATAL UNHANDLED EXCEPTION ***")
    $log.error("e: #{e.inspect}")
    $log.error("at@ #{e.backtrace.join("\n")}")
    raise e
  end
end

#get serialized url
post '/snapshot/serializedurl' do
  begin
    service     = params[:service]
    snapshot_id = params[:snapshotid]

    parse_header
    service_id  = get_service_id(service)

    serialized_url = create_serialized_url(service_id, snapshot_id)

    serialized_url
  rescue Exception => e
    $log.error("*** FATAL UNHANDLED EXCEPTION ***")
    $log.error("e: #{e.inspect}")
    $log.error("at@ #{e.backtrace.join("\n")}")
    raise e
  end
end

#export data
post '/snapshot/exportdata' do
  begin
    service     = params[:service]
    snapshot_id = params[:snapshotid]

    parse_header
    service_id  = get_service_id(service)

    serialized_url = create_serialized_url(service_id, snapshot_id)
    serialized_data = download_data(serialized_url)

    serialized_data
  rescue Exception => e
    $log.error("*** FATAL UNHANDLED EXCEPTION ***")
    $log.error("e: #{e.inspect}")
    $log.error("at@ #{e.backtrace.join("\n")}")
    raise e
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

post '/testheader' do
  begin
    #env['token']
    #request[:token]
    request.port
  rescue Exception => e
    $log.error("*** FATAL UNHANDLED EXCEPTION ***")
    $log.error("e: #{e.inspect}")
    $log.error("at@ #{e.backtrace.join("\n")}")
  end
end

get '/foo' do
  #status, headers, body = call env.merge("PATH_INFO" => '/bar')
  #[status, headers, body.map(&:upcase)]
  #request
  status 418
  headers \
    "Allow"   => "BREW, POST, GET, PROPFIND, WHEN",
    "Refresh" => "Refresh: 20; http://www.ietf.org/rfc/rfc2324.txt"
  body "#{request.env}"
end

get '/bar' do
  body "#{env['HTTP_TOKEN']}"
end

get '/foobar' do
  parse_header
  @token
end

# get memory usage of redis (MB)
get '/redis/memory' do
  $log.info("get redis memory")
  used_memory = ''
  client = load_redis
  $log.info("client info: #{client.info}")
  client.info.each {|i|
    if i[0] == 'used_memory'
      used_memory = i[1]
      break
    end
  }
  body "#{used_memory.to_f / 1024 / 1024}"
end
