# icecp-module-storage

This module is used to store information permanently to disk; currently the only DB implementation is OrientDB, but other implementations are 
possible (e.g. relational, document, etc.). The data to store arrive as messages on a configured channel (see _Commands_) and their assigned 
IDs are published on an acknowledgment channel (see _Attributes_). Additionally, messages are grouped together in [sessions](https://github.com/icecp/icecp-module-storage/wiki/Session-Lifecycle)
and can be [tagged](https://github.com/icecp/icecp-module-storage/wiki/Tagging).


### Install

Clone this repository and run: `mvn install`. Optionally, for troubleshooting, you may want to install OrientDB locally:
  
  1. Download OrientDB 2.1.x GA Community Edition from website (http://orientdb.com/download/) or from Git: 2.1.x branch
      `git clone https://github.com/orientechnologies/orientdb`
      `git checkout 2.1.x`
      `cd orientdb`
      `mvn clean install -DskipTests`
  2. Follow the installation instruction here: (http://orientdb.com/docs/last/Unix-Service.html)  
   if you are using Ubuntu, then you can follow instruction here: (http://www.famvdploeg.com/blog/2013/01/setting-up-an-orientdb-server-on-ubuntu/).  

   
### Run

Use [icecp-tools](https://github.com/icecp/icecp-tools) to load the module in a running [icecp-node](https://github.com/icecp/icecp-node)


### Commands

The storage module receives commands using [icecp-rpc](https://github.com/icecp/icecp-rpc). The following commands can be sent:
- **start** - Start a new session that will listen to a channel and store all messages published on that channel in the DB
- **get** - Retrieve messages by session and publish them to a designated replay channel
- **getTimeSpan** - Get the timestamp range (min and max) for active messages on a specified channel
- **size** - Get the number of messages in a session
- **stop** - Stop a session from recording any more messages
- **queryBySessionId** - Get a list of sessions associated with an actively recording session identifier
- **queryByChannelName** - Get a list of sessions associated with a channel that is being recorded
- **rename** - Replace an existing session with a new session
- **tag** - Tag a set of messages with a tags
- **untag** - Untag a set of messages
- **listTag** - Get a list tags under the specified channel URI
- **deleteByTag** - Delete all messages that are tagged with a specified tag
- **deleteSession** - Delete all messages related to a session
- **deleteMessagesByRange** - Delete all messages between a start and end ID (this relies on the assumption that message IDs assigned by the DB increase)

See [Commands](https://github.com/icecp/icecp-module-storage/wiki/Commands) wiki page for more information.


### Attributes

 - `ack-channel`: the URI of the channel on which acknowledgments for persisted messages are sent on, to `icecp-module-ack` module

This attribute is defined in `configuration/config.json` --> `{"ack-channel" : "uri-of-the-ack-channel"}`


### Messages
 
An Acknowledgment Message or an `AckMessage` for every message that was successfully saved will have the following JSON structure:
```
{uri: "ndn:/some-ack-channel", id: 12345678}
```


### Documentation
 - [Javadoc](https://icecp.github.io/icecp-module-storage/)
 - [Wiki](https://github.com/icecp/icecp-module-storage/wiki)


### License

Copyright &copy; 2016, Intel Corporation 

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0).

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
