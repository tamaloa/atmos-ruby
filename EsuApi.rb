require 'net/http'
require 'net/https'
require 'uri'
require 'time'
require 'base64'
require 'hmac-sha1'

module EsuApi
  class EsuRestApi
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    ID_EXTRACTOR = /\/[0-9a-zA-Z]+\/objects\/([0-9a-f]{44})/
    
    def initialize( host, port, uid, secret )
      @host = host
      @port = port
      @uid = uid
      @secret = Base64.decode64( secret )
      @session = Net::HTTP.new( host, port ).start
      
      @context = "/rest"
    end
    
    
    def create_object( acl, metadata, data, mimetype, hash = nil)
      #uri = URI::parse( "#{@context}/objects" )
      #uri = URI::HTTP.build( "#{@context}/objects" )
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
    
    def delete_object( id )
      uri = URI::HTTP.build( {:host => @host, :port => @port,
      :path => @context + "/objects" + build_resource(id) } )
      
      headers = {}
      
      request = build_request( EsuRestApi::DELETE, uri, headers, nil )
      
      response = @session.request( request )
      
      handle_error( response )
    end
    
    def read_object( id, extent, buffer, checksum = nil )
      uri = URI::HTTP.build( {:host => @host, :port => @port,
      :path => @context + "/objects" + build_resource(id) } )
      
      headers = {}
      
      request = build_request( EsuRestApi::GET, uri, headers, nil )
      
      response = @session.request( request )
      
      handle_error( response )
      
      return response.body
    end
    
  def update_object( id, acl, metadata, data, mimetype, hash = nil)
    #uri = URI::parse( "#{@context}/objects" )
    #uri = URI::HTTP.build( "#{@context}/objects" )
    uri = URI::HTTP.build( {:host => @host, :port => @port,
    :path => @context + "/objects" + build_resource(id) } )
    
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

  #####################
    ## Private Methods ##
    #####################
    private
    
    def process_acl( acl, headers )
      usergrants = []
      groupgrants = []
      acl.each { |grant|
        if( grant.grantee_type == EsuApi::Grantee::USER )
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
        if( value.listable? )
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
      return "/" + identifier
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
      signstring += "\n"
      
      customheaders = {}
      headers.each { |key,value|
        if key == "x-emc-date"
          #skip
        elsif key =~ /^x-emc-/
          customheaders[ key.downcase ] = value
        end
      }
      customheaders.sort()
      first = true
      customheaders.each { |key,value|
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
      end
    end
    
    def sign( string ) 
      value = HMAC::SHA1.digest( @secret, string )
      signature = Base64.encode64( value ).chomp()
      print "String to sign: #{string}\nSignature: #{signature}\nValue: #{value}\n"
      return signature
    end
  end
  
end