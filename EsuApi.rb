require 'net/http'
require 'net/https'
require 'uri'
require 'time'
require 'base64'
require 'hmac-sha1'
require 'nokogiri'

module EsuApi
  class EsuRestApi
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    HEAD = "HEAD"
    ID_EXTRACTOR = /\/[0-9a-zA-Z]+\/objects\/([0-9a-f]{44})/
    OID_TEST = /^[0-9a-f]{44}$/
    PATH_TEST = /^\/.*/
    
    def initialize( host, port, uid, secret )
      @host = host
      @port = port
      @uid = uid
      @secret = Base64.decode64( secret )
      @session = Net::HTTP.new( host, port ).start
      
      @context = "/rest"
    end
    
    
    def create_object( acl, metadata, data, mimetype, hash = nil)
      uri = URI::HTTP.build( {:host => @host, :port => @port,
      :path => @context + "/objects" } )
      
      headers = {}
      if( data == nil )
        data = ""
      end
      
      headers["content-length"] = String(data.length())
      
      if( acl )
        process_acl( acl, headers )
      end
      
      if( metadata )
        process_metadata( metadata, headers )
      end
            
      if( hash )
        update_hash( hash, data, headers )
      end
      
      request = build_request( EsuRestApi::POST, uri, headers, mimetype )
      request.body = data
      
      response = @session.request( request )
      
      handle_error( response )
      
      return ID_EXTRACTOR.match( response["location"] )[1].to_s
    end
      
      
    def create_object_on_path( path, acl, metadata, data, mimetype, hash = nil)
      #uri = URI::parse( "#{@context}/objects" )
      #uri = URI::HTTP.build( "#{@context}/objects" )
      uri = URI::HTTP.build( {:host => @host, :port => @port,
      :path => build_resource( path ) } )
      
      headers = {}
      if( data == nil )
        data = ""
      end
      
      headers["content-length"] = String(data.length())
      
      if( acl )
        process_acl( acl, headers )
      end
      
      if( metadata )
        process_metadata( metadata, headers )
      end
            
      if( hash )
        update_hash( hash, data, headers )
      end
      
      request = build_request( EsuRestApi::POST, uri, headers, mimetype )
      request.body = data
      
      response = @session.request( request )
      
      handle_error( response )
      
      return ID_EXTRACTOR.match( response["location"] )[1].to_s
    end
    
    
    def delete_object( id )
      uri = URI::HTTP.build( {:host => @host, :port => @port,
      :path => build_resource(id) } )
      
      headers = {}
      
      request = build_request( EsuRestApi::DELETE, uri, headers, nil )
      
      response = @session.request( request )
      
      handle_error( response )
    end
    
    def read_object( id, extent, buffer, checksum = nil )
      uri = URI::HTTP.build( {:host => @host, :port => @port,
      :path => build_resource(id) } )
      
      headers = {}
      
      request = build_request( EsuRestApi::GET, uri, headers, nil )
      
      response = @session.request( request )
      
      handle_error( response )
      
      return response.body
    end
    
  def update_object( id, acl, metadata, data, mimetype, hash = nil)
    uri = URI::HTTP.build( {:host => @host, :port => @port,
    :path => build_resource(id) } )
    
    headers = {}
    if( data == nil )
      data = ""
    end
    
    headers["content-length"] = String(data.length())
    
    if( acl )
      process_acl( acl, headers )
    end
    
    if( metadata )
      process_metadata( metadata, headers )
    end
          
    if( hash )
      update_hash( hash, data, headers )
    end
    
    request = build_request( EsuRestApi::PUT, uri, headers, mimetype )
    request.body = data
    
    response = @session.request( request )
    
    handle_error( response )
  end
  
  def get_acl( id )
    uri = URI::HTTP.build( {:host => @host, :port => @port, 
      :path => build_resource(id), :query => "acl" } )
    
    headers = {}
    
    request = build_request( EsuRestApi::GET, uri, headers, nil )
    
    response = @session.request( request )
    
    handle_error( response )
    
    # Parse returned ACLs
    acl = []
    parse_acl( acl, response["x-emc-groupacl"], EsuApi::Grantee::GROUP )
    parse_acl( acl, response["x-emc-useracl"], EsuApi::Grantee::USER )
    
    return acl
    
  end
  
def get_user_metadata( id, tags = nil )
  uri = URI::HTTP.build( {:host => @host, :port => @port, 
    :path => build_resource(id), :query => "metadata/user" } )
  
  headers = {}
  if( tags != nil ) 
    process_tags( tags, headers )
  end
  
  request = build_request( EsuRestApi::GET, uri, headers, nil )
  
  response = @session.request( request )
  
  handle_error( response )
  
  # Parse returned metadata
  meta = {}
  parse_metadata( meta, response["x-emc-meta"], false )
  parse_metadata( meta, response["x-emc-listable-meta"], true )
  
  return meta
end

def get_system_metadata( id, tags = nil )
  uri = URI::HTTP.build( {:host => @host, :port => @port, 
    :path => build_resource(id), :query => "metadata/system" } )
  
  headers = {}
  if( tags != nil ) 
    process_tags( tags, headers )
  end
  
  request = build_request( EsuRestApi::GET, uri, headers, nil )
  
  response = @session.request( request )
  
  handle_error( response )
  
  # Parse returned metadata
  meta = {}
  parse_metadata( meta, response["x-emc-meta"], false )
  
  return meta
end
  
  def delete_user_metadata( id, tags )
    uri = URI::HTTP.build( {:host => @host, :port => @port, 
      :path => build_resource(id), :query => "metadata/user" } )
    
    headers = {}
    headers["x-emc-tags"] = tags.join( "," )
    
    request = build_request( EsuRestApi::DELETE, uri, headers, nil )
    
    response = @session.request( request )
    
    handle_error( response )
  end
  
  def version_object( id )
    uri = URI::HTTP.build( {:host => @host, :port => @port, 
      :path => build_resource(id), :query => "versions" } )
    
    headers = {}
    
    request = build_request( EsuRestApi::POST, uri, headers, nil )
    
    response = @session.request( request )
    
    handle_error( response )
    
    # Parse returned ID
    return ID_EXTRACTOR.match( response["location"] )[1].to_s
  end
  
  def delete_version( vid )
    uri = URI::HTTP.build( {:host => @host, :port => @port, 
      :path => build_resource(vid), :query => "versions" } )
    
    headers = {}
    
    request = build_request( EsuRestApi::DELETE, uri, headers, nil )
    
    response = @session.request( request )
    
    handle_error( response )
  end
  
  def restore_version( id, vid )
    uri = URI::HTTP.build( {:host => @host, :port => @port, 
      :path => build_resource(id), :query => "versions" } )
    
    headers = {}
    headers["x-emc-version-oid"] = vid
    
    request = build_request( EsuRestApi::PUT, uri, headers, nil )
    
    response = @session.request( request )
    
    handle_error( response )
  end
  
  def list_versions(id)
    uri = URI::HTTP.build( {:host => @host, :port => @port, 
      :path => build_resource(id), :query => "versions" } )
    
    headers = {}
    
    request = build_request( EsuRestApi::GET, uri, headers, nil )
    
    response = @session.request( request )
    
    handle_error( response )
    
    # Parse returned IDs
    return parse_version_list( response )
  end
  
  def get_object_metadata( id )
    uri = URI::HTTP.build( {:host => @host, :port => @port, 
    :path => build_resource(id) })
    
    headers = {}

    request = build_request( EsuRestApi::HEAD, uri, headers, nil )
    
    response = @session.request( request )
    
    handle_error( response )
    
    # Parse returned metadata
    om = {}
    meta = {}
    parse_metadata( meta, response["x-emc-meta"], false )
    parse_metadata( meta, response["x-emc-listable-meta"], true )
    om[:meta] = meta
    
    # Parse returned ACLs
    acl = []
    parse_acl( acl, response["x-emc-groupacl"], EsuApi::Grantee::GROUP )
    parse_acl( acl, response["x-emc-useracl"], EsuApi::Grantee::USER )
    om[:acl] = acl
      
    # Get mimetype
    om[:mimetype] = response["content-type"]
      
    return om
    
  end
  
  def get_listable_tags( tag_root = nil )
    uri = URI::HTTP.build( {:host => @host, :port => @port,
    :path => @context + "/objects", :query => "listabletags" } )
    
    headers = {}
    if( tag_root != nil )
      headers["x-emc-tags"] = tag_root
    end

    request = build_request( EsuRestApi::GET, uri, headers, nil )
    
    response = @session.request( request )
    
    handle_error( response )
    
    return parse_tags( response )
    
  end
  

    #####################
    ## Private Methods ##
    #####################
    private
    
    def parse_metadata( meta, value, listable )
      entries = value.split( "," )
      entries.each{ |entvalue|
        nv = entvalue.split( "=", -2 )
        print "#{nv[0].strip}=#{nv[1]}\n"
        m = EsuApi::Metadata.new( nv[0].strip, nv[1], listable )
        meta[nv[0].strip] = m
      }
    end
    
    def parse_acl( acl, value, grantee_type )
      print "Parse: #{grantee_type}\n"
      entries = value.split(",")
      entries.each { |entval|
        nv = entval.split( "=", -2 )
        print "#{nv[0]}=#{nv[1]}\n"
        acl.push( EsuApi::Grant.new( EsuApi::Grantee.new( nv[0].strip, grantee_type ), nv[1] ) )
      }
    end
    
    def process_acl( acl, headers )
      usergrants = []
      groupgrants = []
      acl.each { |grant|
        if( grant.grantee.grantee_type == EsuApi::Grantee::USER )
          usergrants.push( grant )
        else
          groupgrants.push( grant )
        end
      }
      
      if( usergrants.size > 0 )
        headers[ "x-emc-useracl" ] = usergrants.join( "," )
      end

      if( groupgrants.size > 0 )
        headers[ "x-emc-groupacl" ] = groupgrants.join( "," )
      end
        
    end
    
    def process_metadata( meta, headers )
      listable = []
      regular = []
        
      meta.each { |key,value|
        if( value.listable )
          listable.push( value )
        else
          regular.push( value )
        end
      }
      
      if( listable.size > 0 )
        headers["x-emc-listable-meta"] = listable.join( "," )
      end
      
      if( regular.size > 0 )
        headers["x-emc-meta"] = regular.join( "," )
      end
    end
    
    def process_tags( tags, headers )
      headers["x-emc-tags"] = tags.join(",")
    end
    
    def handle_error( response )
      if( Integer(response.code) > 399 ) 
        if( response.body )
          throw "Error executing request: code: " + response.code + " body: " + response.body
        else
          throw "Error executing request: code: " + response.code + " message: " + response.message
        end
      end
    end
    
    
    def build_resource( identifier )
      resource = @context
      if( OID_TEST.match( identifier) )
        return resource + "/objects/" + identifier
      elsif( PATH_TEST.match( identifier ) )
        return resource + "/namespace" + identifier
      else
        throw "Could not determine type of identifier for #{identifier}"
      end
    end
    
    def build_request( method, uri, headers, mimetype )
      if( mimetype == nil )
        mimetype = "application/octet-stream"
      end
      headers["content-type"] = mimetype
      
      # Add request date
      headers["date"] = Time.now().httpdate()
      headers["x-emc-uid"] = @uid
        
      # Build signature string
      signstring = ""
      signstring += method
      signstring += "\n"
      if( mimetype )
        signstring += mimetype
      end
      signstring += "\n"
      if( headers["extent"] )
        signstring += headers["extent"]
      end
      signstring += "\n"
      signstring += headers["date"]
      signstring += "\n"
      
      # Once most users go to Ruby 1.9 we can 
      # make this work with Unicode.
      signstring += URI.unescape( uri.path ).downcase
      if( uri.query ) 
        signstring += "?" + uri.query
      end
      signstring += "\n"
      
      customheaders = {}
      headers.each { |key,value|
        if key == "x-emc-date"
          #skip
        elsif key =~ /^x-emc-/
          customheaders[ key.downcase ] = value
        end
      }
      header_arr = customheaders.sort()
      first = true
      header_arr.each { |key,value|
        # Values are lowercase and whitespace-normalized
        signstring += key + ":" + value.strip.chomp.squeeze( " " ) + "\n"
      }
      
      headers["x-emc-signature"] = sign( signstring.chomp )
        
      print "uri: " + uri.to_s() +"\n" + " path: " + uri.path + "\n"
        
      case method
      when EsuRestApi::GET
        return Net::HTTP::Get.new( uri.request_uri, headers )
      when EsuRestApi::POST
        return Net::HTTP::Post.new( uri.request_uri, headers )
      when EsuRestApi::PUT
        return Net::HTTP::Put.new( uri.request_uri, headers )
      when EsuRestApi::DELETE
        return Net::HTTP::Delete.new( uri.request_uri, headers )
      when EsuRestApi::HEAD
        return Net::HTTP::Head.new( uri.request_uri, headers )
      end
    end
    
    def sign( string ) 
      value = HMAC::SHA1.digest( @secret, string )
      signature = Base64.encode64( value ).chomp()
      print "String to sign: #{string}\nSignature: #{signature}\nValue: #{value}\n"
      return signature
    end
    
    
    #
    # Uses Nokogiri to select the OIDs from the response using XPath
    #
    def parse_version_list( response )
      print( "parse_version_list: #{response.body}\n" )
      v_ids = []
      doc = Nokogiri::XML( response.body )
      
      # Locate OID tags
      doc.xpath( '//xmlns:OID' ).each { |node|
        print( "Found node #{node}\n" )
        v_ids.push( node.content )
      }
      
      return v_ids
    end
    
    def parse_tags( response )
      tags = []
      response["x-emc-listable-tags"].split(",").each { |tag|
        tags.push( tag.strip )
      }
        
      return tags
        
    end
  end
  
  class Grant
    READ = "READ"
    WRITE = "WRITE"
    FULL_CONTROL = "FULL_CONTROL"

    def initialize( grantee, permission )
      @grantee = grantee
      @permission = permission
    end
    
    def to_s()
      print "Grant::to_s()\n"
      return "#{@grantee}=#{@permission}"
    end
    
    def ==(other_grant)
      return @grantee == other_grant.grantee && @permission == other_grant.permission
    end
    
    attr_accessor :grantee, :permission
  end
  
  class Grantee
    USER = "USER"
    GROUP = "GROUP"
    
    def initialize( name, grantee_type )
      @name = name
      @grantee_type = grantee_type
    end
    OTHER = EsuApi::Grantee.new( "other", EsuApi::Grantee::GROUP )
    
    def to_s()
      return @name
    end
    
    def ==(other_grantee)
      return @name == other_grantee.name && @grantee_type == other_grantee.grantee_type
    end
    
    attr_accessor :name, :grantee_type
  end
  
  class Metadata
    def initialize( name, value, listable )
      @name = name
      @value = value
      @listable = listable
    end
    
    def to_s()
      return "#{name}=#{value}"
    end
    
    def ==(other_meta)
      return @name==other_meta.name && @value==other_meta.value && @listable==other_meta.listable
    end
    
    attr_accessor :name, :value, :listable
  end
    
end