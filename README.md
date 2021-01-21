# plugin-googlecloud-stackdriver

![Google Cloud Services](https://spaceone-custom-assets.s3.ap-northeast-2.amazonaws.com/console-assets/icons/cloud-services/google_cloud/Google_Cloud.svg)
**Plugin for Google Cloud Stack Driver**

> SpaceONE's [plugin-google-cloud-services](https://github.com/spaceone-dev/plugin-google-cloud-stackdriver) is a convenient tool to 
get monitoring metrics and metric's data in real time such as 
- CPU Usage
- CPU Utilization
- Disk Write Bytes
- Disk Read Bytes
- more ...

Find us also at [Dockerhub](spaceone/google-cloud-stackdriver)
> Latest stable version : 1.0

Please contact us if you need any further information. 
(<support@spaceone.dev>)

---

## Authentication Overview
Registered service account on SpaceONE must have certain permissions to collect cloud service data 
Please, set authentication privilege for followings:

### Contents

* [Cloud Monitoring](/abs)








#### abs 
#### [Cloud Monitoring](https://cloud.google.com/monitoring/docs/apis)

- Project (monitoring) 
    - Scopes (OAuth)
        - https://www.googleapis.com/auth/cloud-platform
        - https://www.googleapis.com/auth/monitoring
        - https://www.googleapis.com/auth/monitoring.read
        - https://www.googleapis.com/auth/monitoring.write
        - https://www.googleapis.com/auth/cloud-platform
        - https://www.googleapis.com/auth/monitoring
        - https://www.googleapis.com/auth/monitoring.read
        