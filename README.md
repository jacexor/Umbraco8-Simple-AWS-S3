# Umbraco8-Simple-AWS-S3
Simple Amazon S3 Integration for Umbraco 8. Forked from https://github.com/ElijahGlover/Umbraco-S3-Provider/

## Installation & Configuration
Setup by adding following to web.config
```xml
<appSettings>
  <add key="awsBucketHostname" value="" />
  <add key="awsRegion" value="" />
  <add key="awsBucketPrefix" value="media" />
  <add key="awsBucketName" value="" />
  <add key="awsKey" value="" />
  <add key="awsSecret" value="" />
</appSettings>
```
