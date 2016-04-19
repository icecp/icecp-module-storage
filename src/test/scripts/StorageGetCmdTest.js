/**
 * Script to test Get Message Responses of Storage Module.
 */

load("src/test/scripts/icecp-module-storage.js");

print("\r\nSTEP 1: Create Deap Node");
var node = createDeapNode();

print("\r\nSTEP 2: Open Cmd and Reponse Channels to Storage module");
var storageCmdChannel = openChannel(node, storageCmdChannelName,
		icecp.StorageBaseMessage.class, CHANNEL_PERSIST_TIME);

var responseChannel = openChannel(myNode, responseChannelName,
		icecp.StorageBaseResponseMessage.class, CHANNEL_PERSIST_TIME);
var responseChannelName = responseChannel.getName().toString();

print("\r\nSTEP 3: Open Listen Channel.");
var listenChannel = openChannel(myNode, listenChannelName,
		icecp.BytesMessage.class, CHANNEL_PERSIST_TIME);
var listenChannelName = listenChannel.getName().toString();

print("\r\nSTEP 4: Open Replay Channel.");
replayChannel = openChannel(myNode, replayChannelName, icecp.BytesMessage.class,
		CHANNEL_PERSIST_TIME);
replayChannelName = replayChannel.getName().toString();

print("\r\nSTEP 5: Send CMD to Storage Module over Cmd Channel");
var message = createStartMessage(listenChannelName, responseChannelName);
var response = publishMessageWithResponse(message, storageCmdChannel,
		SLEEP_TIME, responseChannel);
// get the session ID from the response...this will be used for other messages.
var sessionId = response.getSessionId();

print("\r\nSTEP 6: Publish messages on listen channel and replay");
subscribeToChannel(replayChannel);

var number_of_messages_sent = 5;
var bytes;

for (var i = 0; i < number_of_messages_sent; ++i) {
	bytes = createMessage(10 * i, 10);
	publishMessage(bytes, listenChannel, SLEEP_TIME);
}

// Retrieving messages with limit = 100, skip = 0
print("\r\nSTEP 7: Get messages and Replay. " + "limit = 100 skip = 0 ");
message = createGetMessage(replayChannelName, sessionId, 100, 0,
		responseChannelName);
response = publishMessageWithResponse(message, storageCmdChannel, SLEEP_TIME,
		responseChannel);

// Retrieving messages with limit = 100, skip > number_of_messages_sent
print("\r\nSTEP 8: Get messages and Replay for skip > number of messages_sent. "
		+ "limit = 100, skip = " + (number_of_messages_sent + 1));
message = createGetMessage(replayChannelName, sessionId, 100,
		number_of_messages_sent + 1, responseChannelName);
response = publishMessageWithResponse(message, storageCmdChannel, SLEEP_TIME,
		responseChannel);

// Retrieving messages with limit = -1, skip = 0.
print("\r\nSTEP 9: Get messages and Replay for " + "limit = 0, skip = 0");
message = createGetMessage(replayChannelName, sessionId, 0, 0,
		responseChannelName);
response = publishMessageWithResponse(message, storageCmdChannel, SLEEP_TIME,
		responseChannel);

// Retrieving messages with limit = -1, skip = 0.
print("\r\nSTEP 10: Get messages and Replay for a negative limit value. "
		+ "limit = -1, skip = 0");
message = createGetMessage(replayChannelName, sessionId, -1, 0,
		responseChannelName);
response = publishMessageWithResponse(message, storageCmdChannel, SLEEP_TIME,
		responseChannel);

// Retrieving messages with limit = 100, skip = -1.
print("\r\nSTEP 11: Get messages and Replay for a negative skip value. "
		+ "limit = 100, skip = -1");
message = createGetMessage(replayChannelName, sessionId, 100, -1,
		responseChannelName);
response = publishMessageWithResponse(message, storageCmdChannel, SLEEP_TIME,
		responseChannel);

// Retrieving messages with limit = null, skip = null.
print("\r\nSTEP 12: Get messages and Replay for a null limit and skip value. "
		+ "limit = null, skip = null" + ", session ID = " + sessionId);
message = createGetMessage(replayChannelName, sessionId, null, null,
		responseChannelName);
response = publishMessageWithResponse(message, storageCmdChannel, SLEEP_TIME,
		responseChannel);

// Retrieving messages with for an invalid session Id.
print("\r\nSTEP 13: Get messages and Replay for Invalid Session Id. "
		+ "limit = 100, skip = 0" + ", session ID = 1234");
message = createGetMessage(replayChannelName, 1234, 100, 0, responseChannelName);
response = publishMessageWithResponse(message, storageCmdChannel, SLEEP_TIME,
		responseChannel);

print("\r\nKeep running so callback can receive messages.")
while (true) {
	java.lang.Thread.sleep(1000);
}
