package com.intel.icecp.module.storage.ack;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 */
public class AckMessageTest {
    private final ObjectMapper o = new ObjectMapper();
    private AckMessage ackMessage;
    private URI incomingChannelUri;
    private long id;

    @Before
    public void setUp() {
        incomingChannelUri = URI.create("ndn:/incoming");
        id = Arrays.hashCode(new byte[]{(byte) 0x00, (byte) 0x01, (byte) 0x02, (byte) 0x03});
        ackMessage = new AckMessage(incomingChannelUri, id);
    }

    @Test
    public void testAckMessageNotNull() throws JsonProcessingException {
        assertNotNull(o.writeValueAsString(ackMessage));
    }

    @Test
    public void testAckMessageGetUri() throws JsonProcessingException {
        assertEquals(incomingChannelUri, ackMessage.getUri());
    }

    @Test
    public void testAckMessageGetId() throws JsonProcessingException {
        assertEquals(id, ackMessage.getId());
    }
}