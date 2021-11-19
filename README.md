# EMSX API Code Samples for Python
---

## Overview

These code samples offer developers an example of how to build solutions using the EMSX API service of the Bloomberg API.

Bloomberg's EMSX API provides access to the EMSX platform programmatically. It allows the user to automate operations that would otherwise be performed manually by a trader. 

The Bloomberg API is the generic mechanism that provides access to Bloomberg services such as EMSX API. The API comes in the form of an SDK which can be downloaded from the [Bloomberg Customer Service Center](https://service.bloomberg.com/). 

It is available in two forms, Desktop API and Server API. The desktop solution requires a logged in Bloomberg Terminal to operate, and the server solution, for EMSX API, requires the Trading API Server solution. Please speak to a Bloomberg representative for futher information.

```
Note: These are code _**samples**_, and should not be considered production ready.  
All samples are configured to use the Beta/UAT service by default.
```

## Bloomberg API functionality

The Bloomberg API is a session and service based proprietary API solution. It is available as an SDK for the following languages:-

   * .NET (including .NET Core)
   * Java
   * Python
   * C++

The `session` provides the connectivity to Bloomberg. Once a session is established, any number of services can be selected, including those of the Trading API group, such as EMSX API, RANK API and IOI API. 

The Bloomberg API supports two fundamental paradigms:-

    * _request/response_: Create a request, populate the field values for that specifc request, send the request to Bloomberg, and Bloomberg will send back a response.
    * _subscription_: The subscription is for streaming data. The client declares an interest in some data (such as their order blotter) and Bloomberg will stream the order data to the client in real-time.

Each `service` exposes a schema that defines both the operations that can be performed (request/response) and the events and fields that can be subscribed to (subscription).

The Bloomberg API is thread-safe and thread-aware, and can be used in either synchronous or asynchronous modes.

> The EMSX API service requires the user to be enabled before use. Please speak to your local Bloomberg representative for futher information.


## Establishing a session

The first step in establishing a session is to set up the `sessionOptions` object:-

   ```
   d_host = "localhost"
   d_port = 8194

   session_options = blpapi.SessionOptions()
   session_options.setServerHost(d_host)
   session_options.setServerPort(d_port)
   ```

The host represent the end-point that will connect you to Bloomberg. This is either `localhost` if using the DAPI implmentation. This will connect the client to Bloomberg over the terminal infrastructure. In the case of a server setup using the Trading API Server solution, this would be the IP address or URL of the server installation.

The port default value is 8194. This value me be changed under advice from Bloomberg for both terminal and server connectivity, if a conflict exists.

Once the `session_options` object has been created, connecting the `session` can be done in either synchronous or asynchronous modes. This is a design choice on the part of the client.


### Synchronous model

In the synchronous model, the client is responsible for polling the session to see if any events are available to be handled. 

   ``` 
   session = blpapi.Session(session_options)

   if not session.start():
      print("Failed to start session.")
      return

   if not session.openService("//blp/emapisvc_beta"):
      print("Failed to open //blp/emapisvc_beta")
      return

   sendCreateOrderRequest(session)

   while not done:
      event = session.nextEvent(500) \# 500 milli second timeout

      ...process the event...
   ```



### Asynchronous model

In the asynchronous model, the client provides an event handler call-back method which is fire every time an event is received.
