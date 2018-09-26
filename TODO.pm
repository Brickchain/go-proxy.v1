TODO list
=========

* Chunking for large messages
* Traffic limiting
* Improve monitoring and logging

In the current implementation a message bus (in memory or redis) is used to pass the messages between the HTTP request handler and the client connection handler.

This is a potential bottleneck and should probably be replaced with a smarter routing method where there is a direct connection between each proxy server instance and the messages are sent directly between them. This requires keeping a shared lookup table of where each connection is and to make sure that all instances can talk to each other.
