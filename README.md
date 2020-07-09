# Triggerflow patch for IBM-PyWren

This repository contains all the necessary files to make the IBM-PyWren library running with the Triggerflow system. Note that this patch will modify the local `pywren-ibm-cloud` installation once installed. 


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

Once installed, you need to create a configuration file for IBM-PyWren. [Follow this instructions](https://github.com/pywren/pywren-ibm-cloud/tree/master/config) to configure an IBM Cloud Functions and IBM Cloud object Storage accounts. The configuration file can be placed in any location, it is not needed to place it in `~/.pywren_config`. For example, you can place it in `~/pywren_triggerflow_config`




