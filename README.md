<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [rest_pubsub](#rest_pubsub)
  - [Using the library](#using-the-library)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# rest_pubsub

Library to work with the the REST based APIs for [Cloud Pub/Sub](https://cloud.google.com/pubsub/docs/reference/rest).

This is a companion to [grpc_pubsub](https://pub.dev/packages/grpc_pubsub) in that the APIs are almost 100% compatible.  The gRPC one is more efficient when running a Dart server, a desktop app, or a mobile app.  However, gRPC isn't supported for PubSub via the browser, so this should be used in when running as a web application.


## Using the library

Add the repo to your Flutter `pubspec.yaml` file.

```
dependencies:
  rest_pubsub: <<version>> 
```

Then run...
```
flutter packages get
```
