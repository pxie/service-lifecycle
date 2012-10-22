require "helpers"

module Worker
  module ServiceLifecycleHelper

  #SERVICE_LIFECYCLE_CONFIG = ENV['VCAP_BVT_DEPLOY_MANIFEST'] || File.join(File.dirname(__FILE__), "service_lifecycle.yml")
  #SERVICE_CONFIG = (YAML.load_file(SERVICE_LIFECYCLE_CONFIG) rescue {"properties"=>{"service_plans"=>{}}})
  #SERVICE_PLAN = ENV['VCAP_BVT_SERVICE_PLAN'] || "free"
  #SERVICE_SNAPSHOT_QUOTA = {}
  #SERVICE_CONFIG['properties']['service_plans'].each do |service,config|
  #  SERVICE_SNAPSHOT_QUOTA[service] = config[SERVICE_PLAN]["configuration"] if config.include?(SERVICE_PLAN)
  #end
  #DEFAULT_SNAPSHOT_QUOTA = 5

  def snapshot_quota(service)
    q = SERVICE_SNAPSHOT_QUOTA[service] || {}
    q = q["lifecycle"] || {}
    q = q["snapshot"] || {}
    q["quota"] || DEFAULT_SNAPSHOT_QUOTA
  end

  def auth_headers
    {"content-type"=>"application/json", "AUTHORIZATION" => @session.token}
  end

  def get_snapshots(service_id)
    easy = Curl::Easy.new("#{@session.TARGET}/services/v1/configurations/#{service_id}/snapshots")
    easy.headers = auth_headers
    easy.resolve_mode =:ipv4
    easy.http_get

    if easy.response_code == 501
      pending "Snapshot extension is disabled, return code=501"
    elsif easy.response_code != 200
      raise "code:#{easy.response_code}, body:#{easy.body_str}"
    end

    resp = easy.body_str
    $log.debug("list snapshots. url: #{easy.url}, resp: #{resp}")
    #resp.should_not == nil
    #JSON.parse(resp)

    resp
  end

  def get_serialized_url(service_id, snapshot_id)
    easy = Curl::Easy.new("#{@session.TARGET}/services/v1/configurations/#{service_id}/serialized/url/snapshots/#{snapshot_id}")
    easy.headers = auth_headers
    easy.resolve_mode =:ipv4
    easy.http_get

    if easy.response_code == 501
      pending "Serialized API is disabled, return code=501"
    elsif easy.response_code != 200
      return nil
    end

    resp = easy.body_str
    result = JSON.parse(resp)
    result["url"]
  end

  def download_data(serialized_url)
    temp_file = Tempfile.new("serialized_data")
    $log.info("The temp file path: #{temp_file.path}")
    File.open(temp_file.path, "wb+") do |f|
      c = Curl::Easy.new(serialized_url)
      c.on_body{|data| f.write(data)}
      c.perform
      #c.response_code.should == 200
      $log.debug("download data. url: #{c.url}, response: #{c.response_code}")
    end

    File.open(temp_file.path) do |f|
      $log.debug("serialized data size: #{f.size / 1024 / 1024}MB")#f.size.should > 0
    end
    serialized_data_file = temp_file
  end

  def import_service_from_url(service_id, serialized_url)
    easy = Curl::Easy.new("#{@session.TARGET}/services/v1/configurations/#{service_id}/serialized/url")
    easy.headers = auth_headers
    payload = {"url" => serialized_url}
    easy.resolve_mode =:ipv4
    easy.http_put(JSON payload)

    resp = easy.body_str
    #resp.should_not == nil
    #job = JSON.parse(resp)
    #job = wait_job(service_id, job["job_id"])
    ##job.should_not == nil
    #snapshot_id = job["result"]["snapshot_id"]
    ##snapshot_id.should_not == nil
    #job
  end

  def import_service_from_data(service_id, serialized_data)
    post_data = []
    post_data << Curl::PostField.content("_method", "put")
    post_data << Curl::PostField.file("data_file", serialized_data.path)

    easy = Curl::Easy.new("#{@session.TARGET}/services/v1/configurations/#{service_id}/serialized/data")
    easy.multipart_form_post = true
    easy.headers = {"AUTHORIZATION" => @session.token}
    easy.resolve_mode =:ipv4
    easy.http_post(post_data)

    resp = easy.body_str
    $log.info("import data. service id: #{service_id}, serialized_data: #{serialized_data.path}, resp: #{resp}")

    #delete the temp file
    serialized_data.unlink

    resp

    #$log.info("Response from import data: #{resp}")
    ##resp.should_not == nil
    #job = JSON.parse(resp)
    #job = wait_job(service_id, job["job_id"])
    ##job.should_not == nil
    #snapshot_id = job["result"]["snapshot_id"]
    ##snapshot_id.should_not == nil
    #job
  end

  def parse_service_id(content, srv_name)
    service_id = nil
    services = JSON.parse content
    services.each do |k, v|
      v.each do |srv|
        if srv["label"] =~ /#{srv_name}/
          service_id = srv["credentials"]["name"]
          break
        end
      end
    end
    service_id
  end

  def create_serialized_url(service_id, snapshot_id)
    easy = Curl::Easy.new("#{@session.TARGET}/services/v1/configurations/#{service_id}/serialized/url/snapshots/#{snapshot_id}")
    easy.headers = auth_headers
    easy.resolve_mode =:ipv4
    easy.http_post ''

    #easy.response_code.should == 200
    resp = easy.body_str
    #$log.debug("create serialized url. response: #{resp.inspect}")
    ##resp.should_not == nil
    #job = JSON.parse(resp)
    #job = wait_job(service_id,job["job_id"])
    #job = JSON.parse(job)
    #job["result"]["url"]
  end

  def post_and_verify_service(service_manifest, app, key, data)
      url = SERVICE_URL_MAPPING[service_manifest[:vendor]]
      app.get_response(:post, "/service/#{url}/#{key}", data)
      app.get_response(:get, "/service/#{url}/#{key}").body_str.should == data
  end

  def verify_service(service_manifest, app, key, data)
      url = SERVICE_URL_MAPPING[service_manifest[:vendor]]
      app.get_response(:get, "/service/#{url}/#{key}").body_str.should == data
  end

  def create_snapshot(service_id)
    url = "#{@session.TARGET}/services/v1/configurations/#{service_id}/snapshots"
    easy = Curl::Easy.new(url)
    easy.headers = auth_headers
    easy.resolve_mode =:ipv4
    easy.http_post

    #easy.response_code.should == 200
    resp = easy.body_str
    #resp.should_not == nil
    $log.info("create snapshot. url: #{url}, hearder: #{auth_headers}, response body: #{easy.body_str}")
    resp
  end

  def get_snapshot(service_id, snapshot_id)
    easy = Curl::Easy.new("#{@session.TARGET}/services/v1/configurations/#{service_id}/snapshots/#{snapshot_id}")
    easy.headers = auth_headers
    easy.resolve_mode =:ipv4
    easy.http_get

    #if easy.response_code != 200
    #  return nil
    #end

    resp = easy.body_str
    #resp.should_not == nil
    JSON.parse(resp)

    resp
  end

  def rollback_snapshot(service_id, snapshot_id)
    easy = Curl::Easy.new("#{@session.TARGET}/services/v1/configurations/#{service_id}/snapshots/#{snapshot_id}")
    easy.headers = auth_headers
    easy.resolve_mode =:ipv4
    easy.http_put ''

    #easy.response_code.should == 200

    resp = easy.body_str
    ##resp.should_not == nil
    #job = JSON.parse(resp)
    #job = wait_job(service_id,job["job_id"])
    ##job.should_not == nil
    ##job["result"]["result"].should == "ok"
    #job
  end

  def delete_snapshot(service_id, snapshot_id)
    easy = Curl::Easy.new("#{@session.TARGET}/services/v1/configurations/#{service_id}/snapshots/#{snapshot_id}")
    easy.headers = auth_headers
    easy.resolve_mode =:ipv4
    easy.http_delete

    #easy.response_code.should == 200
    resp = easy.body_str
    $log.debug("delete snapshot. resp: #{resp}")
    resp
    ##resp.should_not == nil
    #job = JSON.parse(resp)
    #job = wait_job(service_id, job["job_id"])
    ##job.should_not == nil
    ##job["result"]["result"].should == "ok"
    #job
  end

  def wait_job(service_id, job_id)
    timeout = 2 * 60 * 60
    sleep_time = 1
    while timeout > 0
      sleep sleep_time
      timeout -= sleep_time

      job = get_job(service_id, job_id)
      return job.to_json if job_completed?(job)
    end
    # failed
    raise "Time out"
  end

  def get_job(service_id, job_id)
    easy = Curl::Easy.new("#{@session.TARGET}/services/v1/configurations/#{service_id}/jobs/#{job_id}")
    easy.headers = auth_headers
    easy.resolve_mode =:ipv4
    easy.http_get

    resp = easy.body_str
    $log.debug("get job. response: #{resp.inspect}")
    #resp.should_not == nil
    JSON.parse(resp)
  end

  def job_completed?(job)
    return true if job["status"] == "completed" || job["status"] == "failed"
  end



  end
end



