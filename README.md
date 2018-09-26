# Brickchain Tunneling Proxy

The purpose of the proxy is to allow a server to publish a service through HTTP or Websockets *without* the hassle of **opening up an external port** in a firewall. The client, as described below, is the service connected to the proxy, and the proxy is a centrally hosted service.

The proxy converts HTTP and Websocket requests into *messages* that are passed to the proxy client via the websocket connection that the client already has established with the proxy.

The proxy client processes the request and creates a response message that has the same ID as the request and sends it back to the proxy over the websocket connection. If the client fails to respond to the request,there is a timeout of 30 seconds before the HTTP request is answered with an error code of 504 Gateway timeout.

## Tunneling setup

When the client first connects to the proxy it sends a signed **RegistrationRequest** message. The proxy responds with a hostname that is the base32 encoded hash of the signing keys thumbprint plus a *domain name* that the proxy should have a valid certificate (probably a wildcard certificate), and DNS pointer for.

## Messages

The messages are defined in the [messages.go](./messages.go) file, and is used by both the client and the server.

**RegistrationRequest** is created, signed, and sent by the client to the proxy.

**RegistrationResponse** is sent back from the proxy to the client and contains the hostname that the client now can use.

**HTTPRequest** is created when a HTTP request is handled by the proxy on behalf of a client (a third party). The Host header of the request decides which client should handle the message, and them the message is sent to the client.

**HTTPResponse** is what the client needs to respond back with to the proxy. The ID of the response needs to be the same as the ID of the request.

**WSRequest** is a message created when there is a request towards the proxy with the Websocket upgrade headers set.

**WSResponse** is what the client should send back to the proxy when it has handled the WSRequest and is ready to process messages coming over the websocket.

**WSMessage** encapsulates the messages passed back and forth over the websocket established by the WSRequest/WSResponse messages. The ID of the WSMessage needs to be the ID of the WSRequest (can be thought of as the connection ID).

**WSTeardown** is sent by either the proxy or the client when they want to stop the websocket connection. The ID needs to be the connection ID (the ID from the WSRequest).

A **Ping** is sent every 10 seconds if there is no other traffic going over the connection. If the client has not received a ping or other traffic in 20 seconds it should reconnect and register again.

## Limitations

There is a limiter in the proxy to limit the amount of messages for each connection. Each request body and response body ***can not exceed 500kb***.

In the current implementation a message bus (in memory or redis) is used to pass the messages between the HTTP request handler and the client connection handler.

## The code

The server and client are contained in this repository, and both are located the cmd directory.
