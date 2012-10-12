require 'digest/sha1'

module Worker
  module Helper
    MILLION = 1 * 1024 * 1024

    ## generate binary data, unit of size is MB
    def provision_data(size)
      data = Random.new(Time.now.usec).bytes(size * MILLION)
      [data, sha1sum(data)]
    end

    def sha1sum(data)
      Digest::SHA1.hexdigest(data)
    end

    def think(thinktime)
      sleep(rand(20) / 20 * thinktime)
    end
  end
end