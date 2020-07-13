# Triggerflow patch for IBM-PyWren

This repository contains all the necessary files to make the IBM-PyWren library running with the Triggerflow system. Note that this patch will modify the local `pywren-ibm-cloud` installation once applied. 

The Triggerflow patch for IBM-PyWren is shipped with 4 different event sources: *kafka*, *Redis*, *Cloudant* and *IBM Cloud Object Store*. An event source is the place where the termination event of a functions is stored.

To run a workflow as code, the main component is the *Coordinator function*. This function contains all the actual pywren function invocations: `call_async()`, `map()` and `map_reduce()`. Each time The coordinator function invokes a function with one of those methods, it is immediately shutted down. When the invoked function finishes its execution it sends the termination event to the configured event source, then the Triggerflow service receives this event and awakes the Coordinator function. At this point ,the coordinator function, using an event sourcing technique, recovers the state of the execution and continues invoking the other pywren calls, skipping those that are already done.

## Installation

The following guide provides instructions for a the installation and configuration of the IBM-PyWren client. 

1. Using a virutal environment is recommended (Optional).

    ```
    $ python3 -m venv tf_env
    $ source tf_env/bin/activate
    ```

2. Install IBM-PyWren library, version 1.7.0:
   
    ```
    pip install pywren-ibm-cloud==1.7.0
    ```

3. Install the Triggerflow library:
    
    ```
    git clone https://github.com/triggerflow/triggerflow
    python triggerflow/setup.py install
    ```
     
4. Install The Triggerflow patch for IBM-PyWren:

    ```
    git clone https://github.com/triggerflow/pywren-ibm-cloud_tf-patch
    python pywren-ibm-cloud_tf-patch/install_patch.py
    ```

## Configuration

1. Once installed, you need to create a configuration file for IBM-PyWren. [Follow this instructions](https://github.com/pywren/pywren-ibm-cloud/tree/master/config) to configure an IBM Cloud Functions and IBM Cloud object Storage accounts. It is recommended to place the `pywren_config` file in the root directory of your project that contains the Triggerflow scripts.

2. Edit your pywren config file and add the `triggerflow` section with your access details to your *Triggerflow* deployment. You must provide here the event source service (sink):
    ```yaml
    triggerflow:
        endpoint: http://127.0.0.1:8080
        user: admin
        password: admin
        workspace: MyPywrenWorkspace
        sink: kafka
    ```

 3. Add in your pywren config file the access details to the Triggerflow event source service. In this example kafka:
     ```yaml
     kafka:
        broker_list: [127.0.0.1:9092]
     ```

4. If you configured the triggerflow service with another event source, you must add the access details in the pywren config file, for example:
    ```yaml
    cloudant:
        url: http://127.0.0.1:5984
        cloudant_user: admin
        auth_token: admin
      
    redis:
        host: 127.0.0.1
        port: 6379
        password: G6pSd9mQzeR5Dzuw2JIJjAVZWK6v
    ```
    

## Usage

1. Create a Triggerflow workspace:
    ```python
    with open('pywren_config.yaml', 'r') as config_file:
        tf_config = yaml.safe_load(config_file)

    tf = Triggerflow(endpoint=tf_config['triggerflow']['endpoint'],
                     user=tf_config['triggerflow']['user'],
                     password=tf_config['triggerflow']['password'])

    kafka_event_source = KafkaEventSource(**tf_config['kafka'])

    tf.create_workspace(workspace=tf_config['triggerflow']['workspace'],
                        global_context={'ibm_cf': tf_config['ibm_cf']},
                        event_source=kafka_event_source)
    ```

2. Create the coordinator function, which contains all the PyWren calls, and define your functions:
    ```python
    def my_function(x):
        time.sleep(x)
        return x + 3
    
    def main(args): # Coordinator function
        pywren_config = args.get('config')
        execution_id = args.get('execution_id', None)
        pw = pywren.ibm_cf_executor(config=args['config'],
                                    execution_id=args['execution_id'],
                                    log_level='INFO')
        res = 0
        pw.call_async(my_function, int(res))
        res = pw.get_result()
        pw.call_async(my_function, int(res))
        res = pw.get_result()
    ```

 3. Deploy and run the coordinator function:
     ```python
     tf_exec = TriggerflowExecutor()
     tf_exec.run('pywren_tf_test', main)
     ```

Find a complete example in [examples/test_call_async.py](examples/test_call_async.py)