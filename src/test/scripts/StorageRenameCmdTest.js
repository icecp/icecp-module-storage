/**
 * Script to test Rename Message Responses of Storage Module.
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

// Retrieving Size before rename.
print("\r\nSTEP 7: Retrieve Size of Messages on Channel before RENAME. ");
message = createSizeMessage(responseChannelName, sessionId);
response = publishMessageWithResponse(message, storageCmdChannel, SLEEP_TIME,
		responseChannel);

print("\r\nSTEP 8: Rename Session");
message = createRenameMessage(responseChannelName, sessionId);
response = publishMessageWithResponse(message, storageCmdChannel, SLEEP_TIME,
		responseChannel);
// get the new session ID from the response.
var newSessionId = response.getNewSessionId();

print("\r\nSTEP 9: Publish messages on listen channel and replay after Rename.");
var number_of_messages_sent = 8;
var bytes;
for (var i = 0; i < number_of_messages_sent; ++i) {
	bytes = createMessage(10 * i, 10);
	publishMessage(bytes, listenChannel, SLEEP_TIME);
}

print("\r\nSTEP 10: Retrieve Size of Messages on Channel after RENAME. ");
message = createSizeMessage(responseChannelName, newSessionId);
response = publishMessageWithResponse(message, storageCmdChannel, SLEEP_TIME,
		responseChannel);

print("\r\nKeep running so callback can receive messages.")
while (true) {
	java.lang.Thread.sleep(1000);
}
