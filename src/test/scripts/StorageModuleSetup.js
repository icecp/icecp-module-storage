/**
 * Script to test the basic infrastructure of Storage Module.
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

print("\r\nSTEP 3: Open Listen Channel to Listen");
var listenChannel = openChannel(myNode, listenChannelName,
		icecp.BytesMessage.class, CHANNEL_PERSIST_TIME);
var listenChannelName = listenChannel.getName().toString();

print("\r\nSTEP 4: Open Replay Channel to Listen");
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

var bytes = createMessage(0, 10);
publishMessage(bytes, listenChannel, SLEEP_TIME);
bytes = createMessage(11, 10);
publishMessage(bytes, listenChannel, SLEEP_TIME);
bytes = createMessage(21, 10);
publishMessage(bytes, listenChannel, SLEEP_TIME);
bytes = createMessage(31, 10);
publishMessage(bytes, listenChannel, SLEEP_TIME);
bytes = createMessage(31, 10);
publishMessage(bytes, listenChannel, SLEEP_TIME);

message = createGetMessage(replayChannelName, sessionId, 100, 0,
		responseChannelName);
response = publishMessageWithResponse(message, storageCmdChannel, SLEEP_TIME,
		responseChannel);

message = createGetMessage(replayChannelName, sessionId, 100, 2,
		responseChannelName);
response = publishMessageWithResponse(message, storageCmdChannel, SLEEP_TIME,
		responseChannel);

message = createGetMessage(replayChannelName, sessionId, 0, 0,
		responseChannelName);
response = publishMessageWithResponse(message, storageCmdChannel, SLEEP_TIME,
		responseChannel);

// message = createGetMessage(replayChannelName, sessionId, null, null,
// responseChannelName);
// response = publishMessageWithResponse(message, storageCmdChannel, SLEEP_TIME,
// responseChannel);

print("\r\nSTEP 7: Query Listen Channel");
message = createQueryMessage(listenChannelName, responseChannelName);
response = publishMessageWithResponse(message, storageCmdChannel, SLEEP_TIME,
		responseChannel);

print("\r\nSTEP 8: Get Size of Messages on a Channel");
message = createSizeMessage(responseChannelName, sessionId);
response = publishMessageWithResponse(message, storageCmdChannel, SLEEP_TIME,
		responseChannel);

print("\r\nSTEP 9: Delete invalid session ID");
message = createDeleteMessage(responseChannelName, 1234);
response = publishMessageWithResponse(message, storageCmdChannel, SLEEP_TIME,
		responseChannel);

print("\r\nSTEP 10: Delete valid session ID");
message = createDeleteMessage(responseChannelName, sessionId);
response = publishMessageWithResponse(message, storageCmdChannel, SLEEP_TIME,
		responseChannel);

print("\r\nSTEP 11: Rename Session");
message = createRenameMessage(responseChannelName, sessionId);
response = publishMessageWithResponse(message, storageCmdChannel, SLEEP_TIME,
		responseChannel);
// get the new session ID from the response.
var newSessionId = response.getNewSessionId();

print("\r\nSTEP 12: Stop a Session");
message = createStopMessage(responseChannelName, newSessionId);
response = publishMessageWithResponse(message, storageCmdChannel, SLEEP_TIME,
		responseChannel);

print("Keep running so callback can receive messages.")
while (true) {
	java.lang.Thread.sleep(1000);
}
