require 'json'
require 'logger'
require 'elasticsearch'
require 'yaml'
require 'csv'

class CustLog
  @stdout = nil
  @fileout = nil 
  def initialize(filename , level)
     @stdout = Logger.new(STDOUT)
     File.delete(filename) if File.exist?(filename)
     @fileout = Logger.new(filename)
       
    case level
      when "INFO"
        puts "Logging at INFO level in #{filename}"
        @stdout.level = Logger::INFO
        @fileout.level = Logger::INFO
      when "DEBUG"
        puts "Logging at DEBUG level in #{filename}"
        @stdout.level = Logger::DEBUG
        @fileout.level = Logger::INFO
      else
        puts "invalid log level"
        exit
    end 
  end
  
  def info(mess)
    @stdout.info(mess)
    @fileout.info(mess)
  end
  
  def debug(mess)
    @stdout.debug(mess)
    @fileout.debug(mess)
  end
  
end

def pullFromPath( arrPath , x )
  if  arrPath.size == 1 and !x.is_a?(Array)
    c = arrPath[0]
    if x.has_key?(c)
      return x[c]
    else
      return nil
    end
  else
    if x.is_a?(Hash)
      c = arrPath.shift
      if x.has_key?(c)
        return pullFromPath( arrPath , x[c])
      else
         return nil
      end
    else
      #TODO: to do to extract multiple arrays
      #return pullFromPath( arrPath , x[0])
      arr = []
      x.each do |m|
        arr << pullFromPath( arrPath , m)
      end
      return arr
    end
  end
end

config = ARGV[0].nil? ? "config.yml" : ARGV[0]

$configs = {"INCLUDE"=>[],"EXCLUDE"=>[]}.merge(YAML.load_file(config))

######### INNOVATE FULL INCLUDES FROM FIELDS
$configs["INCLUDE"] = []
$configs["OUTPUT"]["COLS"].each do |k,p|
  $configs["INCLUDE"] << p
end
  
#$log = Logger.new(STDOUT)
$log = CustLog.new($configs["LOG_FILE"] , $configs["LOG_LEVEL"])


$log.info("CONFIGS: #{$configs.to_json}")

client = Elasticsearch::Client.new hosts: $configs["HOST"],  log: false

client.transport.reload_connections!

tmp =  (client.indices.status index: $configs["INDEX"])

shards = tmp["indices"][$configs["INDEX"]]["shards"].size
$log.debug( "# of shards: #{shards}" )
tmp = client.count index: $configs["INDEX"] , type: $configs["DOC_TYPE"], body: $configs["QRY"] 
$log.debug("# of docs: #{tmp}")
expected =  tmp["count"].to_i  
numScrolls = (expected / ($configs["PAGE_SIZE"].to_i * shards )).ceil
$log.debug("# of scrolls ~#{numScrolls}")
# Open the "view" of the index with the `scan` search_type
r = client.search index: $configs["INDEX"], search_type: 'scan', scroll: $configs["SCROLL_TIME"], size: $configs["PAGE_SIZE"], type: $configs["DOC_TYPE"] , body: $configs["QRY"] , :_source_include => $configs["INCLUDE"] , :_source_exclude => $configs["EXCLUDE"]

# Call the `scroll` API until empty results are returned
CSV.open($configs["OUTPUT"]["FILE"], "w",{:col_sep => $configs["OUTPUT"]["DELIM"]}) do |csv|
  keys = []
  keys << "doc_id" if $configs["OUTPUT"].has_key?("INCLUDE_ID") && $configs["OUTPUT"]["INCLUDE_ID"] == true
  keys = keys + $configs["OUTPUT"]["COLS"].keys
  csv << keys
  csv.flush()
  i = 1
  starttime = Time.new
  $log.info "Scroll #{i} of ~#{numScrolls} started (logging every 100)..."
  while r = client.scroll(scroll_id: r['_scroll_id'], scroll: '5m') and not r['hits']['hits'].empty? do
    r['hits']['hits'].each do |x|
      row = []
      current_id = x["_id"]  
      entitypull = {}  
      $configs["OUTPUT"]["COLS"].each do |k,p|
        
        pathoutput = nil
        
        pathoutput = pullFromPath(p.split("."),x["_source"])
        
        if !$configs["OUTPUT"]["TRANSFORM"].nil? && !$configs["OUTPUT"]["TRANSFORM"][k].nil?
          funct = $configs["OUTPUT"]["TRANSFORM"][k].gsub("$inputData","'#{pathoutput}'")
          pathoutput =  eval(funct)
        end
      
        pathoutput = [pathoutput] if not pathoutput.is_a?(Array) #convert to array if single value
        entitypull[k] = pathoutput
      end
      
      #find max rows
      maxrows = 0
      entitypull.each do |k,r|
        maxrows = r.size if r.size > maxrows
      end
      
      #create rows
      for row_num in 1..maxrows
        row = []
        row << current_id if $configs["OUTPUT"].has_key?("INCLUDE_ID") && $configs["OUTPUT"]["INCLUDE_ID"] == true
        entitypull.each do |c,v|
          if row_num <= v.size
            row << v[row_num - 1]
          else
            if $configs["OUTPUT"].has_key?("KEYS") and $configs["OUTPUT"]["KEYS"].include?(c)
              row << v[0]
            else
              row << nil
            end
          end
        end
       # towrite = false
   #     row.each do |q|
   #       if q.nil? == false and q != ""
   #         towrite = true
   #       end
  #      end
        csv << row # if towrite
      end
      
    end
    $log.info "...scroll #{i} of ~#{numScrolls} complete.  Est Time remaining: #{  (( ((Time.new- starttime) / i ) * (numScrolls-i) ) / 60 ).ceil  } min remaining" if i % 100 == 0
    csv.flush()
    i += 1
    $log.info "Scroll #{i} of ~#{numScrolls} started (logging every 100)..." if i % 100 == 0
  end
  $log.info "...Scroll #{i} of ~#{numScrolls} empty."
end