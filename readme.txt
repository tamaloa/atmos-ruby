-----------------------------
| Ruby REST API for EMC ESU |
-----------------------------

This API allows Ruby developers to easily connect to EMC's ESU 
Storage.  It handles all of the low-level tasks such as generating and signing 
requests, connecting to the server, and parsing server responses.

Requirements
------------
 * Ruby 1.8.7+
 * atmos-ruby also requires the following GEMs to run:
  * Nokogiri (for XML parsing) 
    * Installation: http://nokogiri.org/tutorials/installing_nokogiri.html (MIT License)
  * ruby-hmac (to compute HMAC signatures) (MIT License)
    * Installation: gem install ruby-hmac

Usage
-----
To use the API, require 'EsuApi'

In order to use the API, you need to construct an instance of the EsuRestApi
class.  This class contains the parameters used to connect to the server.

esu = EsuApi::EsuRestApi.new( "host", port, "uid", "shared secret" );

Where host is the hostname or IP address of an ESU node that you're authorized
to access, port is the IP port number used to connect to the server (generally
80 for HTTP), UID is the username to connect as, and the shared secret is the
shared secret key assigned to the UID you're using.  The UID and shared secret
are available from your ESU tennant administrator.  The secret key should be
a base-64 encoded string as shown in the tennant administration console, e.g
"jINDh7tV/jkry7o9D+YmauupIQk=".

After you have created your EsuRestApi object, you can use the methods on the
object to manipulate data in the cloud.  For instance, to create a new, empty
object in the cloud, you can simply call:

id = esu.createObject( nil, nil, nil, nil );

The createObject method will return an ObjectId you can use in subsequent calls
to modify the object.
