Service Lifecycle Stress Testing Tool
================

Steps
-------------
1. deploy your environment, and enable service lifecycle feature
2. Edit config/users.yml
    - input proper control domain
    - input valid email template name, password, total account number,
    - remove users field and all fields under it, if you want to create users
3. run ```rake createusers```. It will create test users according to information
   you input into config/users.yml file
4. Edit config/load-manifest.yml to define your load
    - Edit services:tested_inst:name, version, plan, provider to determine which service you want to tested
    - Edit cusers field to define how many concurrent users will be run in stress testing
    - Edit other load actions properly
5. run ```rake tests``` to start your testing
6. all logs are stored in ./testing.log file
7. when tests finished, one error rate report will be outputed to stdout, and log file
