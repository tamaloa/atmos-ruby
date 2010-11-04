# Copyright © 2010, EMC Corporation.
# Redistribution and use in source and binary forms, with or without modification, 
# are permitted provided that the following conditions are met:
#
#     + Redistributions of source code must retain the above copyright notice, 
#       this list of conditions and the following disclaimer.
#     + Redistributions in binary form must reproduce the above copyright 
#       notice, this list of conditions and the following disclaimer in the 
#       documentation and/or other materials provided with the distribution.
#     + The name of EMC Corporation may not be used to endorse or promote 
#       products derived from this software without specific prior written 
#       permission.
#
#      THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 
#      "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED 
#      TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR 
#      PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS 
#      BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
#      CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
#      SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
#      INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
#      CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
#      ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
#      POSSIBILITY OF SUCH DAMAGE.

require 'net/http'
require 'net/https'
require 'uri'
require 'time'
require 'base64'
require 'hmac-sha1'
require 'nokogiri'
require 'cgi'

# The EsuApi Module provides access to the EMC Atmos REST APIs
module EsuApi
  # The EsuRestApi object creates a connection to the Atmos REST API
  #
  # Note that this class is not thread-safe.  Each instance maps one-to-one to
  # a Net::HTTP session.  Therefore, each instance must be kept in a thread
  # local or pooled using something like common-pool: 
  # http://www.pluitsolutions.com/projects/common-pool
  #
  class EsuRestApi
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    HEAD = "HEAD"
    ID_EXTRACTOR = /\/[0-9a-zA-Z]+\/objects\/([0-9a-f]{44})/
    OID_TEST = /^[0-9a-f]{44}$/
    PATH_TEST = /^\/.*/

    # Creates a new connection
    #
    # * host - the access point host
    # * port - the port to connect with
    # * uid - the Atmos UID
    # * secret - the base64-encoded secret key for the UID
    def initialize( host, port, uid, secret )
      @host = host
      @port = port
      @uid = uid
      @secret = Base64.decode64( secret )
      @session = Net::HTTP.new( host, port ).start

      @context = "/rest"
    end

    # Creates a new object in Atmos and returns its Object ID
    #
    # * acl - the ACL to apply to the object.  May be nil.
    # * metadata - the object's initial metadata as a hash of name => Metadata,
    # may be nil.
    # * data - data for the object.  If nil, a zero-length object will be
    # created.
    # * mimetype - the object's mimetype.  If not specified, will default
    # to application/octet-stream.
    # * hash - optional.  If specified, the object will be populated with
    # the hash of the data passed.  If uploading a file in multiple chunks,
    # the same hash object should be passed to the subsequent update calls.
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

    # Creates an object on the given path.  The path must start with a slash
    # (/) character.  When complete, returns the ID of the new object.
    #
    # * path - the path in the namespace, e.g. "/myfile.txt"
    # * acl - the ACL to apply to the object.  May be nil.  Should be an 
    # #Array of #Grant objects
    # * metadata - the object's initial metadata as a hash of name => Metadata,
    # may be nil.
    # * data - data for the object.  If nil, a zero-length object will be
    # created.
    # * mimetype - the object's mimetype.  If not specified, will default
    # to application/octet-stream.
    # * hash - optional.  If specified, the object will be populated with
    # the hash of the data passed.  If uploading a file in multiple chunks,
    # the same hash object should be passed to the subsequent update calls.
    def create_object_on_path( path, acl, metadata, data, mimetype, hash = nil)
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

    # Deletes an object.
    #
    # * id - A string containing either an Object ID or a path
    def delete_object( id )
      uri = URI::HTTP.build( {:host => @host, :port => @port,
        :path => build_resource(id) } )

      headers = {}

      request = build_request( EsuRestApi::DELETE, uri, headers, nil )

      response = @session.request( request )

      handle_error( response )
    end

    # Reads an object's content.
    # 
    # * id - A string containing either an Object ID or an object path.
    # * extent - If nil, the entire object will be returned.  Otherwise, only
    # the requested extent will be returned
    # * checksum - Optional.  If specified, the data will be added to the
    # given checksum object.  Note that Atmos currently only supports
    # read checksums on erasure coded objects.  If you're reading a file
    # in sequential chunks, pass the same checksum object to each request.
    # When complete, check the checksum object's expected_value against
    # it's to_s() value.
    def read_object( id, extent, checksum = nil )
      uri = URI::HTTP.build( {:host => @host, :port => @port,
        :path => build_resource(id) } )

      headers = {}
        
      if( extent != nil )
        headers["range"] = "#{extent}"
      end

      request = build_request( EsuRestApi::GET, uri, headers, nil )

      response = @session.request( request )

      handle_error( response )
      
      if( checksum != nil )
        checksum.update( response.body )
        if( response["x-emc-wschecksum"] != nil )
          checksum.expected_value = response["x-emc-wschecksum"]
        end
      end
      

      return response.body
    end

    # Updates an object in Atmos
    #
    # * id - a String containing either an object ID or an object path.
    # * acl - the ACL to apply to the object.  May be nil.  Should be an 
    # #Array of #Grant objects
    # * metadata - the object's initial metadata as a hash of name => Metadata,
    # may be nil.
    # * data - data for the object.  If nil, a zero-length object will be
    # created.
    # * mimetype - the object's mimetype.  If not specified, will default
    # to application/octet-stream.
    # * hash - optional.  If specified, the object will be populated with
    # the hash of the data passed.  If uploading a file in multiple chunks,
    # the same hash object should be passed to the subsequent update calls.
    def update_object( id, acl, metadata, data, extent, mimetype, hash = nil)
      uri = URI::HTTP.build( {:host => @host, :port => @port,
        :path => build_resource(id) } )

      headers = {}
      if( data == nil )
        data = ""
      end

      headers["content-length"] = String(data.length())

      if( extent != nil )
        headers["range"] = "${extent}"
      end

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

    # Gets an object's ACL.  Returns an #Array containing #Grant objects.
    #
    # * id - a #String containing either an object ID or an object path
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

    # Gets the user metadata on an object.  Returns a #Hash of metadata name =>
    # #Metadata objects.
    #
    # * id - a String containing either an object ID or an object path
    # * tags - Optional.  If specified, an Array of Strings containing the
    # user metadata tags to fetch from the server.
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

    # Gets the system metadata on an object.  Returns a #Hash of metadata name =>
    # #Metadata objects, e.g. ctime, atime, mtime, size
    #
    # * id - a String containing either an object ID or an object path
    # * tags - Optional.  If specified, an Array of Strings containing the
    # system metadata tags to fetch from the server.
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

    # Deletes user metadata from an object
    #
    # * id - a String containing an object ID or an object path
    # * tags - an Array containing the names of the user metadata elements
    # to delete from the object.
    def delete_user_metadata( id, tags )
      uri = URI::HTTP.build( {:host => @host, :port => @port,
        :path => build_resource(id), :query => "metadata/user" } )

      headers = {}
      headers["x-emc-tags"] = tags.join( "," )

      request = build_request( EsuRestApi::DELETE, uri, headers, nil )

      response = @session.request( request )

      handle_error( response )
    end

    # Creates a new version of an object.  Returns the ID of the new version.
    # Note that versions are immutable.  See #restore_version.
    #
    # * id - a String containing an object ID or an object path.
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

    # Deletes an object version.  Note that you'll get an access error if
    # you pass a version ID to #delete_object
    #
    # * vid - a String containing an object version ID
    def delete_version( vid )
      uri = URI::HTTP.build( {:host => @host, :port => @port,
        :path => build_resource(vid), :query => "versions" } )

      headers = {}

      request = build_request( EsuRestApi::DELETE, uri, headers, nil )

      response = @session.request( request )

      handle_error( response )
    end

    # Restores ("promotes") a version to the base object content, i.e. the
    # object's content will be replaced with the contents of the version.
    #
    # * id - a String containing the object ID or object path of the base
    # object
    # * vid - a String containing the object version ID to replace the base
    # content with
    def restore_version( id, vid )
      uri = URI::HTTP.build( {:host => @host, :port => @port,
        :path => build_resource(id), :query => "versions" } )

      headers = {}
      headers["x-emc-version-oid"] = vid

      request = build_request( EsuRestApi::PUT, uri, headers, nil )

      response = @session.request( request )

      handle_error( response )
    end

    # Lists the versions of an object.  Returns an Array of Strings containing
    # the object version IDs.
    # 
    # * id - a String containing the object ID or object path of the base
    # object
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

    # Fetches the user metadata, system metadata, mimetype, and ACL for an
    # object.  Returned as a hash with the key symbols:
    # * :meta
    # * :acl
    # * :mimetype
    #
    # * id - A String containing an object ID or object path
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

    # Returns the listable tags present in the system.
    #
    # * tag_root - Optional.  If specified, only the listable tags under
    # the root are returned.  If omitted, the root tags will be returned.
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

    # Returns a #ServiceInformation object containing the version of Atmos
    # the client is connected to.
    def get_service_information()
      uri = URI::HTTP.build( {:host => @host, :port => @port,
        :path => @context + "/service"} )

      headers = {}

      request = build_request( EsuRestApi::GET, uri, headers, nil )

      response = @session.request( request )

      handle_error( response )

      return parse_service_information( response )
    end

    # Generates a pre-signed URL to read an object that can be shared with 
    # external users or systems.
    #
    # * id - a String containing an object ID or object path
    # * expires - a #Time object containing the expiration date and time in UTC
    def get_shareable_url( id, expires )
      uri = URI::HTTP.build( {:host => @host, :port => @port,
        :path => build_resource(id) })

      sb = "GET\n"
      sb += uri.path.downcase+ "\n"
      sb += @uid + "\n"
      sb += String(expires.to_i())

      signature = sign( sb )
      uri.query = "uid=#{CGI::escape(@uid)}&expires=#{expires.to_i()}&signature=#{CGI::escape(signature)}"

      return uri

    end

    # Lists the contents of a directory.  Returns an Array of #DirectoryEntry
    # objects.
    #
    # * dir - a String containing a directory path.  Note that directory paths
    # must end with a slash ('/') character.
    def list_directory( dir )
      if !/^\/.*\/$/.match( dir )
        throw "Invalid directory '#{dir}'.  Directories must start and end with a slash (/)"
      end

      uri = URI::HTTP.build( {:host => @host, :port => @port,
        :path => build_resource(dir) } )

      headers = {}

      request = build_request( EsuRestApi::GET, uri, headers, nil )

      response = @session.request( request )

      handle_error( response )

      return parse_directory( response, dir )
    end

    # Lists the objects tagged with the given listable metadata tag.  Returns
    # an Array of object IDs.
    #
    # * tag - the tag whose contents to list.
    def list_objects( tag )
      uri = URI::HTTP.build( {:host => @host, :port => @port,
        :path => @context + "/objects" } )

      headers = {}
      headers["x-emc-tags"] = tag

      request = build_request( EsuRestApi::GET, uri, headers, nil )

      response = @session.request( request )

      handle_error( response )

      return parse_object_list( response )
    end

    # Lists the objects tagged with the given listable metadata tag.  Returns
    # a Hash of object ID => #ObjectMetadata elements 
    #
    # * tag - the tag whose contents to list
    def list_objects_with_metadata( tag )
      uri = URI::HTTP.build( {:host => @host, :port => @port,
        :path => @context + "/objects" } )

      headers = {}
      headers["x-emc-tags"] = tag
      headers["x-emc-include-meta"] = "1"

      request = build_request( EsuRestApi::GET, uri, headers, nil )

      response = @session.request( request )

      handle_error( response )

      return parse_object_list_with_metadata( response )

    end

    # Gets the list of user metadatda tags on an object
    #
    # * id - a String containing the object ID or object path
    def list_user_metadata_tags( id )
      uri = URI::HTTP.build( {:host => @host, :port => @port,
        :path => build_resource(id), :query => "metadata/tags" } )

      headers = {}

      request = build_request( EsuRestApi::GET, uri, headers, nil )

      response = @session.request( request )

      handle_error( response )

      # Parse returned metadata
      tags = []
      parse_tag_list( tags, response["x-emc-tags"] )
      parse_tag_list( tags, response["x-emc-listable-tags"] )

      return tags
    end

    # Renames an object in the namespace
    #
    # * source - the path of the source object
    # * destination - the path of the destination object
    # * overwrite - if true, the destination will be overwritten if it exists.
    # If false, the operation will fail if the destination exists.  Note that
    # overwriting an object is asynchronous; it may take a few seconds for the
    # destination object to be replaced.
    def rename( source, destination, overwrite = false )
      uri = URI::HTTP.build( {:host => @host, :port => @port,
        :path => build_resource(source), :query => "rename" } )

      headers = {}
      headers["x-emc-path"] = destination
      if( overwrite )
        headers["x-emc-force"] = "#{overwrite}"
      end

      request = build_request( EsuRestApi::POST, uri, headers, nil )

      response = @session.request( request )

      handle_error( response )

    end

    #####################
    ## Private Methods ##
    #####################
    private

    def parse_metadata( meta, value, listable )
      entries = value.split( "," )
      entries.each{ |entvalue|
        nv = entvalue.split( "=", -2 )
        #print "#{nv[0].strip}=#{nv[1]}\n"
        m = EsuApi::Metadata.new( nv[0].strip, nv[1], listable )
        meta[nv[0].strip] = m
      }
    end

    def parse_acl( acl, value, grantee_type )
      #print "Parse: #{grantee_type}\n"
      entries = value.split(",")
      entries.each { |entval|
        nv = entval.split( "=", -2 )
        #print "#{nv[0]}=#{nv[1]}\n"
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
      if( headers["range"] )
        signstring += headers["range"]
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

      #print "uri: " + uri.to_s() +"\n" + " path: " + uri.path + "\n"

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
      #print "String to sign: #{string}\nSignature: #{signature}\nValue: #{value}\n"
      return signature
    end

    #
    # Uses Nokogiri to select the OIDs from the response using XPath
    #
    def parse_version_list( response )
      #print( "parse_version_list: #{response.body}\n" )
      v_ids = []
      doc = Nokogiri::XML( response.body )

      # Locate OID tags
      doc.xpath( '//xmlns:OID' ).each { |node|
        #print( "Found node #{node}\n" )
        v_ids.push( node.content )
      }

      return v_ids
    end

    def parse_service_information( response )
      doc = Nokogiri::XML( response.body )

      # Locate atmos version
      return EsuApi::ServiceInformation.new( doc.xpath('//xmlns:Atmos')[0].content )
    end

    def parse_tags( response )
      tags = []
      parse_tag_list( tags, response["x-emc-listable-tags"] )
      return tags
    end

    def parse_directory( response, dir )
      #print( "parse_directory #{response.body}\n")
      doc = Nokogiri::XML( response.body )
      entries = []

      doc.xpath( '//xmlns:DirectoryEntry' ).each { |entry|
        oid = entry.xpath( './xmlns:ObjectID' )[0].content
        fname = entry.xpath( './xmlns:Filename' )[0].content
        ftype = entry.xpath( './xmlns:FileType' )[0].content

        if( ftype == 'directory' )
          fname += '/'
        end
        #print "found #{dir+fname}\n"
        entries.push( EsuApi::DirectoryEntry.new( oid, dir+fname, fname, ftype ) )
      }

      return entries
    end

    def parse_object_list( response )
      doc = Nokogiri::XML( response.body )
      objects = []
      doc.xpath( '//xmlns:ObjectID' ).each { |entry|
        objects.push( entry.content )
      }

      return objects
    end

    def parse_object_list_with_metadata( response )
      #print( "Objects with Metadata response: #{response.body}\n")
      doc = Nokogiri::XML( response.body )
      objects = {}

      doc.xpath( '//xmlns:Object').each { |entry|
        oid = entry.xpath( './xmlns:ObjectID' )[0].content
        smeta = parse_object_metadata_xml( entry, './xmlns:SystemMetadataList/xmlns:Metadata', false )
        umeta = parse_object_metadata_xml( entry, './xmlns:UserMetadataList/xmlns:Metadata', true )

        om = EsuApi::ObjectMetadata.new(oid,smeta,umeta)
        objects[oid] = om

      }

      return objects
    end

    def parse_object_metadata_xml( entry, selector, parse_listable )
      meta = {}

      entry.xpath( selector ).each { |mentry|
        name = mentry.xpath( './xmlns:Name' )[0].content
        value = mentry.xpath( './xmlns:Value' )[0].content
        listable = false
        if( parse_listable )
          listable = mentry.xpath( './xmlns:Listable' )[0].content == "true"
        end
        meta[name] = EsuApi::Metadata.new( name, value, listable )
      }

      return meta
    end

    def parse_tag_list( tags, value )
      value.split(",").each() { |tag|
        tags.push( tag.strip() )
      }
    end
    
    def update_hash( hash, data, headers )
      hash.update( data )
      headers["x-emc-wschecksum"] = "#{hash}"
    end
  end
  
  class Extent
    def initialize( offset, size )
      @offset = offset
      @size = size
    end
    
    def to_s()
      eend = offset + size - 1
      return "Bytes=#{offset}-#{eend}"
    end
    
    attr_accessor :offset, :size
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
      #print "Grant::to_s()\n"
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

  class ServiceInformation
    def initialize( atmos_version )
      @atmos_version = atmos_version
    end

    attr_accessor :atmos_version
  end

  class DirectoryEntry
    def initialize( oid, path, filename, filetype )
      @id = oid
      @path = path
      @filename = filename
      @filetype = filetype
    end

    def ==(other_entry)
      return @path == other_entry.path
    end

    attr_accessor :id, :path, :filename, :filetype
  end

  class ObjectMetadata
    def initialize( oid, smeta, umeta )
      @id = oid
      @system_metadata = smeta
      @user_metadata = umeta
    end

    attr_accessor :id, :system_metadata, :user_metadata
  end
  
  class Checksum
    SHA0 = "SHA0"
    
    def initialize( algorithm )
      @algorithm = algorithm
      @hash = EsuApi::SHA0.new();
      @offset = 0;
      @expected_value = ""
    end
    
    def update( data )
       @offset += data.length()
       @hash.hashUpdate( data )
    end
    
    def to_s()
      value = @hash.clone().hashFinal(nil)
      
      hval = ""
      value.each_byte { |b|
        hval += "%.2x" % b
      }
      return "#{@algorithm}/#{@offset}/#{hval}"
    end
    
    attr_accessor :expected_value
  end

  class SHA0

    BLOCK_SIZE = 64

    def initialize()
      @state = []
      @constants = []
      @buffer = ""
      @state[0] = 0x67452301
      @state[1] = 0xefcdab89
      @state[2] = 0x98badcfe
      @state[3] = 0x10325476
      @state[4] = 0xc3d2e1f0

      @constants[0] = 0x5a827999
      @constants[1] = 0x6ed9eba1
      @constants[2] = 0x8f1bbcdc
      @constants[3] = 0xca62c1d6

      @counter = 0
    end

    # Creates a deep copy of the object.  This allows you to get the
    # current hash value at various offsets without disrupting the
    # hash in progress, e.g.
    # <code>
    # sha = EsuApi::SHA0.new()
    # sha.hashUpdate( block1 )
    # shacopy = sha.clone()
    # partialHash = shacopy.hashFinal(nil)
    # sha.hashUpdate( block2 )
    # ...
    # </code>
    def clone()
      copy = EsuApi::SHA0.new()

      copy.state[0] = @state[0]
      copy.state[1] = @state[1]
      copy.state[2] = @state[2]
      copy.state[3] = @state[3]
      copy.state[4] = @state[4]

      copy.counter = @counter

      copy.buffer = @buffer+"" # Clone the string

      return copy
    end

    def hashUpdate( data )
      # Break up into 64 byte chunks.
      i=0

      while( i<data.length )
        if( data.length - i + @buffer.length >= BLOCK_SIZE )
          usedBytes = SHA0::BLOCK_SIZE-@buffer.length
          @buffer += data.slice( i, usedBytes )

          internalHashUpdate( @buffer )
          @counter += SHA0::BLOCK_SIZE << 3
          @buffer = ""
          i+= usedBytes
        else
          # Save remaining bytes for next chunk
          @buffer += data.slice( i, data.length-i )
          i += data.length-i
        end
      end
    end

    def hashFinal( data )
      if( data == nil )
        data = ""
      end

      # Consume up to the last block
      hashUpdate( data )
      @counter += @buffer.length << 3

      # Append the bits 1000 0000
      @buffer += 0x80.chr

      # See if we have enough room to pad out the final block
      if( @buffer.length > SHA0::BLOCK_SIZE-8 )
        while( @buffer.length < SHA0::BLOCK_SIZE )
          @buffer+=0.chr
        end
        internalHashUpdate( @buffer )
        @buffer = ""

        # Write a zero buffer.
        (0..SHA0::BLOCK_SIZE-9).each{ |i|
          @buffer[i] = 0.chr
        }
      end

      # Expand the buffer out to a block size
      while( @buffer.length < SHA0::BLOCK_SIZE-8 )
        @buffer += 0.chr
      end

      # Append the bit count (8 bytes) to buffer
      carr = []
      carr.push( 0 )
      carr.push( @counter )
      @buffer += carr.pack( "N*" )

      # Process the final block
      internalHashUpdate( @buffer )

      #      var output:ByteArray = new ByteArray()
      #      output.writeUnsignedInt( state[0] )
      #      output.writeUnsignedInt( state[1] )
      #      output.writeUnsignedInt( state[2] )
      #      output.writeUnsignedInt( state[3] )
      #      output.writeUnsignedInt( state[4] )
      output = @state.pack( "N*" )

      return output
    end

    def internalHashUpdate( data )
      # Expand the buffer into an array of uints
      nblk = data.unpack( "N*" )
      
      # Expand into an array of 80 uints
      (16..79).each{ |i|
        nblk[i] = nblk[i-3] ^ nblk[i-8] ^ nblk[i-14] ^ nblk[i-16]
      }
      
      # Do the rounds
      a = truncate(@state[0])
      b = truncate(@state[1])
      c = truncate(@state[2])
      d = truncate(@state[3])
      e = truncate(@state[4])

      e = truncadd( e, rol( a, 5 ) + f1(b, c, d) + nblk[0] )
      b =rol( b, 30 )
      d = d = truncadd( d, rol( e, 5 ) + f1(a, b, c) + nblk[1] )
      a =rol( a, 30 )
      c = c = truncadd( c, rol( d, 5 ) + f1(e, a, b) + nblk[2] )
      e =rol( e, 30 )
      b = truncadd( b, rol( c, 5 ) + f1(d, e, a) + nblk[3] )
      d =rol( d, 30 )
      a = truncadd( a, rol( b, 5 ) + f1(c, d, e) + nblk[4] )
      c =rol( c, 30 )
      e = truncadd( e, rol( a, 5 ) + f1(b, c, d) + nblk[5] )
      b =rol( b, 30 )
      d = d = truncadd( d, rol( e, 5 ) + f1(a, b, c) + nblk[6] )
      a =rol( a, 30 )
      c = c = truncadd( c, rol( d, 5 ) + f1(e, a, b) + nblk[7] )
      e =rol( e, 30 )
      b = truncadd( b, rol( c, 5 ) + f1(d, e, a) + nblk[8] )
      d =rol( d, 30 )
      a = truncadd( a, rol( b, 5 ) + f1(c, d, e) + nblk[9] )
      c =rol( c, 30 )
      e = truncadd( e, rol( a, 5 ) + f1(b, c, d) + nblk[10] )
      b =rol( b, 30 )
      d = d = truncadd( d, rol( e, 5 ) + f1(a, b, c) + nblk[11] )
      a =rol( a, 30 )
      c = truncadd( c, rol( d, 5 ) + f1(e, a, b) + nblk[12] )
      e =rol( e, 30 )
      b = truncadd( b, rol( c, 5 ) + f1(d, e, a) + nblk[13] )
      d =rol( d, 30 )
      a = truncadd( a, rol( b, 5 ) + f1(c, d, e) + nblk[14] )
      c =rol( c, 30 )
      e = truncadd( e, rol( a, 5 ) + f1(b, c, d) + nblk[15] )
      b =rol( b, 30 )
      d = d = truncadd( d, rol( e, 5 ) + f1(a, b, c) + nblk[16] )
      a =rol( a, 30 )
      c = truncadd( c, rol( d, 5 ) + f1(e, a, b) + nblk[17] )
      e =rol( e, 30 )
      b = truncadd( b, rol( c, 5 ) + f1(d, e, a) + nblk[18] )
      d =rol( d, 30 )
      a = truncadd( a, rol( b, 5 ) + f1(c, d, e) + nblk[19] )
      c =rol( c, 30 )
      e = truncadd( e, rol( a, 5 ) + f2(b, c, d) + nblk[20] )
      b =rol( b, 30 )
      d = d = truncadd( d, rol( e, 5 ) + f2(a, b, c) + nblk[21] )
      a =rol( a, 30 )
      c = truncadd( c, rol( d, 5 ) + f2(e, a, b) + nblk[22] )
      e =rol( e, 30 )
      b = truncadd( b, rol( c, 5 ) + f2(d, e, a) + nblk[23] )
      d =rol( d, 30 )
      a = truncadd( a, rol( b, 5 ) + f2(c, d, e) + nblk[24] )
      c =rol( c, 30 )
      e = truncadd( e, rol( a, 5 ) + f2(b, c, d) + nblk[25] )
      b =rol( b, 30 )
      d = d = truncadd( d, rol( e, 5 ) + f2(a, b, c) + nblk[26] )
      a =rol( a, 30 )
      c = truncadd( c, rol( d, 5 ) + f2(e, a, b) + nblk[27] )
      e =rol( e, 30 )
      b = truncadd( b, rol( c, 5 ) + f2(d, e, a) + nblk[28] )
      d =rol( d, 30 )
      a = truncadd( a, rol( b, 5 ) + f2(c, d, e) + nblk[29] )
      c =rol( c, 30 )
      e = truncadd( e, rol( a, 5 ) + f2(b, c, d) + nblk[30] )
      b =rol( b, 30 )
      d = d = truncadd( d, rol( e, 5 ) + f2(a, b, c) + nblk[31] )
      a =rol( a, 30 )
      c = truncadd( c, rol( d, 5 ) + f2(e, a, b) + nblk[32] )
      e =rol( e, 30 )
      b = truncadd( b, rol( c, 5 ) + f2(d, e, a) + nblk[33] )
      d =rol( d, 30 )
      a = truncadd( a, rol( b, 5 ) + f2(c, d, e) + nblk[34] )
      c =rol( c, 30 )
      e = truncadd( e, rol( a, 5 ) + f2(b, c, d) + nblk[35] )
      b =rol( b, 30 )
      d = d = truncadd( d, rol( e, 5 ) + f2(a, b, c) + nblk[36] )
      a =rol( a, 30 )
      c = truncadd( c, rol( d, 5 ) + f2(e, a, b) + nblk[37] )
      e =rol( e, 30 )
      b = truncadd( b, rol( c, 5 ) + f2(d, e, a) + nblk[38] )
      d =rol( d, 30 )
      a = truncadd( a, rol( b, 5 ) + f2(c, d, e) + nblk[39] )
      c =rol( c, 30 )
      e = truncadd( e, rol( a, 5 ) + f3(b, c, d) + nblk[40] )
      b =rol( b, 30 )
      d = d = truncadd( d, rol( e, 5 ) + f3(a, b, c) + nblk[41] )
      a =rol( a, 30 )
      c = truncadd( c, rol( d, 5 ) + f3(e, a, b) + nblk[42] )
      e =rol( e, 30 )
      b = truncadd( b, rol( c, 5 ) + f3(d, e, a) + nblk[43] )
      d =rol( d, 30 )
      a = truncadd( a, rol( b, 5 ) + f3(c, d, e) + nblk[44] )
      c =rol( c, 30 )
      e = truncadd( e, rol( a, 5 ) + f3(b, c, d) + nblk[45] )
      b =rol( b, 30 )
      d = d = truncadd( d, rol( e, 5 ) + f3(a, b, c) + nblk[46] )
      a =rol( a, 30 )
      c = truncadd( c, rol( d, 5 ) + f3(e, a, b) + nblk[47] )
      e =rol( e, 30 )
      b = truncadd( b, rol( c, 5 ) + f3(d, e, a) + nblk[48] )
      d =rol( d, 30 )
      a = truncadd( a, rol( b, 5 ) + f3(c, d, e) + nblk[49] )
      c =rol( c, 30 )
      e = truncadd( e, rol( a, 5 ) + f3(b, c, d) + nblk[50] )
      b =rol( b, 30 )
      d = d = truncadd( d, rol( e, 5 ) + f3(a, b, c) + nblk[51] )
      a =rol( a, 30 )
      c = truncadd( c, rol( d, 5 ) + f3(e, a, b) + nblk[52] )
      e =rol( e, 30 )
      b = truncadd( b, rol( c, 5 ) + f3(d, e, a) + nblk[53] )
      d =rol( d, 30 )
      a = truncadd( a, rol( b, 5 ) + f3(c, d, e) + nblk[54] )
      c =rol( c, 30 )
      e = truncadd( e, rol( a, 5 ) + f3(b, c, d) + nblk[55] )
      b =rol( b, 30 )
      d = d = truncadd( d, rol( e, 5 ) + f3(a, b, c) + nblk[56] )
      a =rol( a, 30 )
      c = truncadd( c, rol( d, 5 ) + f3(e, a, b) + nblk[57] )
      e =rol( e, 30 )
      b = truncadd( b, rol( c, 5 ) + f3(d, e, a) + nblk[58] )
      d =rol( d, 30 )
      a = truncadd( a, rol( b, 5 ) + f3(c, d, e) + nblk[59] )
      c =rol( c, 30 )
      e = truncadd( e, rol( a, 5 ) + f4(b, c, d) + nblk[60] )
      b =rol( b, 30 )
      d = d = truncadd( d, rol( e, 5 ) + f4(a, b, c) + nblk[61] )
      a =rol( a, 30 )
      c = truncadd( c, rol( d, 5 ) + f4(e, a, b) + nblk[62] )
      e =rol( e, 30 )
      b = truncadd( b, rol( c, 5 ) + f4(d, e, a) + nblk[63] )
      d =rol( d, 30 )
      a = truncadd( a, rol( b, 5 ) + f4(c, d, e) + nblk[64] )
      c =rol( c, 30 )
      e = truncadd( e, rol( a, 5 ) + f4(b, c, d) + nblk[65] )
      b =rol( b, 30 )
      d = d = truncadd( d, rol( e, 5 ) + f4(a, b, c) + nblk[66] )
      a =rol( a, 30 )
      c = truncadd( c, rol( d, 5 ) + f4(e, a, b) + nblk[67] )
      e =rol( e, 30 )
      b = truncadd( b, rol( c, 5 ) + f4(d, e, a) + nblk[68] )
      d =rol( d, 30 )
      a = truncadd( a, rol( b, 5 ) + f4(c, d, e) + nblk[69] )
      c =rol( c, 30 )
      e = truncadd( e, rol( a, 5 ) + f4(b, c, d) + nblk[70] )
      b =rol( b, 30 )
      d = d = truncadd( d, rol( e, 5 ) + f4(a, b, c) + nblk[71] )
      a =rol( a, 30 )
      c = truncadd( c, rol( d, 5 ) + f4(e, a, b) + nblk[72] )
      e =rol( e, 30 )
      b = truncadd( b, rol( c, 5 ) + f4(d, e, a) + nblk[73] )
      d =rol( d, 30 )
      a = truncadd( a, rol( b, 5 ) + f4(c, d, e) + nblk[74] )
      c =rol( c, 30 )
      e = truncadd( e, rol( a, 5 ) + f4(b, c, d) + nblk[75] )
      b =rol( b, 30 )
      d = d = truncadd( d, rol( e, 5 ) + f4(a, b, c) + nblk[76] )
      a =rol( a, 30 )
      c = truncadd( c, rol( d, 5 ) + f4(e, a, b) + nblk[77] )
      e =rol( e, 30 )
      b = truncadd( b, rol( c, 5 ) + f4(d, e, a) + nblk[78] )
      d =rol( d, 30 )
      a = truncadd( a, rol( b, 5 ) + f4(c, d, e) + nblk[79] )
      c =rol( c, 30 )

      # Update state
      @state[0] = truncate( a + @state[0] )
      @state[1] = truncate( b + @state[1] )
      @state[2] = truncate( c + @state[2] )
      @state[3] = truncate( d + @state[3] )
      @state[4] = truncate( e + @state[4] )
      
    end
    
    # Roll left; truncate to 32 bits.
    def rol( val, steps )
      return truncate( val << steps )|truncate( val >> 32-steps )
    end
    
    # Truncates to 32 bits to prevent Fixnum from becoming Bignum
    def truncate( val )
      return val & 0xffffffff
    end
    
    # Truncate-and-add function
    def truncadd( v1, v2, v3=0, v4=0 )
      return truncate( v1+v2+v3+v4 )
    end

    # Round 1 mix function
    def f1( a, b, c )
      return truncate((c^(a&(b^c))) + @constants[0])
    end

    # Round 2 mix function
    def f2( a, b, c )
      return truncate((a^b^c) + @constants[1])
    end

    # Round 3 mix function
    def f3( a, b, c )
      return truncate(((a&b)|(c&(a|b))) + @constants[2])
    end

    # Round 4 mix function
    def f4( a, b, c )
      return truncate((a^b^c) + @constants[3])
    end

    attr_accessor :state, :buffer, :counter
  end
end