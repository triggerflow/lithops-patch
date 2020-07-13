# Triggerflow patch for IBM-PyWren

This repository contains all the necessary files to make the IBM-PyWren library running with the Triggerflow system. Note that this patch will modify the local `pywren-ibm-cloud` installation once applied. 


## Installation

The following guide provides instructions for a the installation and configuration of the IBM-PyWren client. 

1. Using a virutal environment is recommended (Optional).

    ```
    $ python3 -m venv tf_env
    $ source tf_env/bin/activate
    ```

2. Install IBM-PyWren library, version 1.7.0:
   
    ```
    pip3 install pywren-ibm-cloud==1.7.0
    ```

3. Install the Triggerflow library:
    
    ```
    git clone https://github.com/triggerflow/triggerflow
    python3 triggerflow/setup.py install
    ```
     
4. Install The Triggerflow patch for IBM-PyWren:

    ```
    git clone https://github.com/triggerflow/pywren-ibm-cloud_tf-patch
    python3 pywren-ibm-cloud_tf-patch/install_patch.py
    ```

## Configuration

1. Once installed, you need to create a configuration file for IBM-PyWren. [Follow this instructions](https://github.com/pywren/pywren-ibm-cloud/tree/master/config) to configure an IBM Cloud Functions and IBM Cloud object Storage accounts. It is recommended to place the `pywren_config` in the root location of your project that contains the Triggeflow scripts.

2. Edit your pywren config file and add the `triggerflow` section with your access details to your *Triggerflow* deployment. You must provide here the event source service (sink):
    ```yaml
    triggerflow:
    endpoint: http://127.0.0.1:8080
    user: admin
    password: admin
    sink: kafka
    ```

 3. Add in your pywren config file the access details to the Triggerflow event source service. In this example kafka:
     ```yaml
     kafka:
        broker_list: [169.45.237.66:9092]
     ```

4. If you configured the triggerflow service with another event source, you must add the access details in the pywren config file, for example:
    ```yaml
    cloudant:
        cloudant_user: admin
        auth_token: admin
        url: http://127.0.0.1:5984
      
    redis:
        host: 127.0.0.1
        port: 6379
        password: G6pSd9mQzeR5Dzuw2JIJjAVZWK6v
    ```
    





